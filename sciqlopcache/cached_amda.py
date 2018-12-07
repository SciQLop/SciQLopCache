from .amda import AMDA, extract_header
import os

import jsonpickle
import pandas as pds
from datetime import datetime
from .cache import Cache, CacheEntry
from .datetime_range import DateTimeRange
import uuid
import pathlib

import logging
log = logging.getLogger(__name__)


class CachedAMDA(AMDA):
    def __init__(self, WSDL='AMDA/public/wsdl/Methods_AMDA.wsdl',
                 server_url="http://amda.irap.omp.eu",
                 data_folder='/tmp/amdacache'
                 ):
        super(CachedAMDA, self).__init__(WSDL, server_url, data_folder + '/amda_inventory.json')
        self.data_folder = data_folder
        self.cache = Cache(data_folder + '/db.json')
        self.headers_files = data_folder + '/headers.json'
        if os.path.exists(self.headers_files):
            with open(self.headers_files, 'r') as f:
                self.headers = jsonpickle.loads(f.read())
        else:
            self.headers = {}
        pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)

    def _save(self):
        super(CachedAMDA, self)._save()
        with open(self.headers_files, 'w') as f:
            f.write(jsonpickle.dumps(self.headers))
        self.cache._save()

    def __del__(self):
        self._save()

    def add_to_cache(self, parameter_id: str, dt_range: DateTimeRange, df: pds.DataFrame):
        fname = self.data_folder + '/' + str(uuid.uuid4())
        if df is not None:
            df.to_pickle(fname)
            self.cache.add_entry(parameter_id, CacheEntry(dt_range, fname))
        else:
            self.cache.add_entry(parameter_id, CacheEntry(dt_range, None))

    def get_header(self, parameter_id, method="REST", **kwargs):
        if parameter_id in self.headers:
            return self.headers[parameter_id]
        else:
            header = extract_header(super(CachedAMDA, self)._get_header_(parameter_id, method))
            self.headers[parameter_id] = header
            return header

    def get_parameter(self, start_time, stop_time, parameter_id, method="REST", **kwargs):
        if type(start_time) is str:
            start_time = datetime.fromisoformat(start_time)
        if type(stop_time) is str:
            stop_time = datetime.fromisoformat(stop_time)
        result = None
        if parameter_id in self.cache:
            entries = self.cache.get_entries(parameter_id, DateTimeRange(start_time, stop_time))
            for e in entries:
                log.debug(f'''Cache hit! {e.dt_range}''')
                if e.data_file is not None:
                    df = pds.read_pickle(e.data_file)
                    if result is None:
                        result = df
                    else:
                        if result.index[0] > df.index[-1]:
                            result = pds.concat([df, result])
                        else:
                            result = pds.concat([result, df])
            miss = self.cache.get_missing_ranges(parameter_id, DateTimeRange(start_time, stop_time))
            for r in miss:
                log.debug(f'''Missing interval {r}''')
                df = super(CachedAMDA, self).get_parameter(r.start_time, r.stop_time, parameter_id, method, **kwargs)
                self.add_to_cache(parameter_id, r, df)
                if df is not None:
                    if result is None:
                        result = df
                    else:
                        if result.index[0] > df.index[-1]:
                            result = pds.concat([df, result])
                        else:
                            result = pds.concat([result, df])
        else:
            result = super(CachedAMDA, self).get_parameter(start_time, stop_time, parameter_id, method, **kwargs)
            self.add_to_cache(parameter_id, DateTimeRange(start_time, stop_time), result)
        if type(result) is pds.DataFrame:
            try:
                result = result[start_time:stop_time]
            except:
                log.debug(f'''can't slice dataframe, slice: {start_time}->{stop_time}  | dataframe : {result.index[0]}->{result.index[-1]}''')
        return result

    def get_parameter_as_txt(self, start_time, stop_time, parameter_id, method="REST", **kwargs):
        if type(start_time) is str:
            start_time = datetime.fromisoformat(start_time)
        if type(stop_time) is str:
            stop_time = datetime.fromisoformat(stop_time)
        data = self.get_parameter(start_time, stop_time, parameter_id, method, **kwargs)
        header = self.get_header(parameter_id)
        txt = header.format(interval_start=start_time.isoformat(), interval_stop=stop_time.isoformat()) + '\n'
        data.index = data.index.format(formatter=lambda x: x.isoformat())
        txt += data.to_string(index_names=False, header=False,
                              formatters={i: "{:.3f}".format for i in range(1, data.shape[1])}
                              )
        return txt
