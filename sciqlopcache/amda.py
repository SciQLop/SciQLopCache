import os
import jsonpickle
import requests
from zeep import Client
import pandas as pds
from datetime import datetime
import xmltodict
from .cache import Cache,CacheEntry,DateTimeRange
import uuid
import pathlib
import urllib.request

class AMDA_soap:
    def __init__(self, server_url="http://amda.irap.omp.eu", WSDL='AMDA/public/wsdl/Methods_AMDA.wsdl', strict=True):
        self.soap_client = Client(server_url + '/' + WSDL)
        self.server_url = server_url

    def get_parameter(self, **kwargs):
        resp = self.soap_client.service.getParameter(**kwargs).__json__()
        if resp["success"]:
            return resp["dataFileURLs"][0]
        else:
            return None

    def get_token(self):
        url = self.server_url + "/php/rest/auth.php?"
        r = requests.get(url)
        return r.text

    def get_obs_data_tree(self):
        resp = self.soap_client.service.getObsDataTree().__json__()
        if resp["success"]:
            return resp["WorkSpace"]["LocalDataBaseParameters"]
        else:
            return None


def listify(list_or_obj):
    if type([]) == type(list_or_obj):
        return list_or_obj
    else:
        return [list_or_obj]


class AMDA_REST:
    def __init__(self, server_url="http://amda.irap.omp.eu"):
        self.server_url = server_url

    def get_parameter(self, **kwargs):
        url = self.server_url + "/php/rest/getParameter.php?"
        for key, val in kwargs.items():
            url += key + "=" + str(val) + "&"
        r = requests.get(url)
        print(url)
        if (r.json()['success']):
            return r.json()['dataFileURLs']
        return ''

    def get_token(self):
        url = self.server_url + "/php/rest/auth.php?"
        r = requests.get(url)
        return r.text

    def get_obs_data_tree(self):
        url = self.server_url + "/php/rest/getObsDataTree.php"
        r = requests.get(url)
        return r.text.split(">")[1].split("<")[0]


class AMDA:

    def __init__(self, WSDL='AMDA/public/wsdl/Methods_AMDA.wsdl', server_url="http://amda.irap.omp.eu"):
        self.METHODS = {
            "REST": AMDA_REST(server_url=server_url),
            "SOAP": AMDA_soap(server_url=server_url, WSDL=WSDL)
        }

    def get_token(self, method="SOAP", **kwargs):
        return self.METHODS[method.upper()].get_token()

    def _get_parameter_url(self, start_time, stop_time, parameter_id, method="SOAP", **kwargs):
        token = self.get_token()
        if type(start_time) is datetime:
            start_time = start_time.isoformat()
        if type(stop_time) is datetime:
            stop_time = stop_time.isoformat()
        url = self.METHODS[method.upper()].get_parameter(
            startTime=start_time, stopTime=stop_time, parameterID=parameter_id, token=token, **kwargs)
        return url

    def _get_header_(self, start_time, stop_time, parameter_id, method="SOAP", **kwargs):
        url = self._get_parameter_url(start_time, stop_time, parameter_id, method, **kwargs)
        with urllib.request.urlopen(url) as data:
            lines = data.readlines()
            lines = [l for l in lines if '#' in l]
            return lines

    def get_parameter(self, start_time, stop_time, parameter_id, method="SOAP", **kwargs):
        url = self._get_parameter_url(start_time, stop_time, parameter_id, method, **kwargs)
        if url is not None:
            print(url)
            return pds.read_csv(url, delim_whitespace=True, comment='#', parse_dates=True, infer_datetime_format=True,
                                index_col=0, header=None)
        return None

    def get_obs_data_tree(self, method="SOAP") -> dict:
        datatree = xmltodict.parse(requests.get(
            self.METHODS[method.upper()].get_obs_data_tree()).text)
        for mission in datatree["dataRoot"]["dataCenter"]["mission"]:
            if 'instrument' in mission:
                for instrument in listify(mission["instrument"]):
                    if 'dataset' in instrument:
                        for dataset in listify(instrument.get('dataset',[])):
                            for parameter in listify(dataset['parameter']):
                                if 'component' in parameter:
                                    parameter['component'] = {
                                        comp["@name"]: comp for comp in listify(parameter['component'])
                                    }
                            dataset['parameter'] = {
                                param["@name"]: param for param in listify(dataset['parameter'])}

                        instrument['dataset'] = {
                            dataset["@name"]: dataset for dataset in listify(instrument['dataset'])}
                mission["instrument"] = {
                    instrument["@name"]: instrument for instrument in listify(mission["instrument"])}
        datatree["dataRoot"]["dataCenter"]["mission"] = {
            mission["@name"]: mission for mission in datatree["dataRoot"]["dataCenter"]["mission"]}
        return datatree


def extract_header(content: str) -> str:
    lines = content.split('\n')
    for index,_ in enumerate(lines):
        if '# INTERVAL_START' in lines[index]:
            lines[index] = '# INTERVAL_START : {interval_start}'
        if '# INTERVAL_STOP' in lines[index]:
            lines[index] = '# INTERVAL_STOP : {interval_stop}'
    return '\n'.join(lines)


class CachedAMDA(AMDA):
    def __init__(self, WSDL='AMDA/public/wsdl/Methods_AMDA.wsdl',
                 server_url="http://amda.irap.omp.eu",
                 data_folder='/tmp/amdacache'
                 ):
        super(CachedAMDA, self).__init__(WSDL, server_url)
        self.data_folder = data_folder
        self.cache = Cache(data_folder+'/db.json')
        self.headers_files = data_folder + '/headers.json'
        if os.path.exists(self.headers_files):
            with open(self.headers_files, 'r') as f:
                self.headers = jsonpickle.loads(f.read())
        else:
            self.headers = {}
        pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)

    def __del__(self):
        with open(self.self.headers_files + '/headers.json', 'w') as f:
            f.write(jsonpickle.dumps(self.headers))

    def add_to_cache(self, parameter_id: str, dt_range: DateTimeRange , df: pds.DataFrame):
        fname = self.data_folder + '/' + str(uuid.uuid4())
        df.to_pickle(fname)
        self.cache.add_entry(parameter_id, CacheEntry(dt_range, fname))

    def get_header(self, parameter_id, method="REST", **kwargs):
        if parameter_id in self.headers:
            pass
        else:
            header = super(CachedAMDA, self)._get_header_()

    def get_parameter(self, start_time, stop_time, parameter_id, method="REST", **kwargs):
        result = None
        if parameter_id in self.cache:
            entries = self.cache.get_entries(parameter_id, DateTimeRange(start_time, stop_time))
            for e in entries:
                df = pds.read_pickle(e.data_file)
                if result is None:
                    result = df
                else:
                    result = pds.concat([result, df])
            miss = self.cache.get_missing_ranges(parameter_id, DateTimeRange(start_time, stop_time))
            for r in miss:
                df = super(CachedAMDA, self).get_parameter(r.start_time, r.stop_time, parameter_id, method, **kwargs)
                self.add_to_cache(parameter_id, r, df)
                if result is None:
                    result = df
                else:
                    result = pds.concat([result, df])
        else:
            result = super(CachedAMDA, self).get_parameter(start_time, stop_time, parameter_id, method, **kwargs)

            self.add_to_cache(parameter_id, DateTimeRange(start_time, stop_time), result)

        return result

