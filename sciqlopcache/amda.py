import os
from typing import Optional

import jsonpickle
import requests
from zeep import Client
import pandas as pds
from datetime import datetime, timedelta
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

    class ObsDataTreeParser:
        @staticmethod
        def node_to_dict(node, **kwargs):
            d = {key.replace('@', ''): value for key, value in node.items() if type(value) is str}
            d.update(kwargs)
            return d

        @staticmethod
        def enter_nodes(node, storage, **kwargs):
            for key, value in storage.items():
                if key in node:
                    for subnode in listify(node[key]):
                        name = subnode['@xml:id']
                        kwargs[key] = name
                        value[name] = AMDA.ObsDataTreeParser.node_to_dict(subnode, **kwargs)
                        AMDA.ObsDataTreeParser.enter_nodes(subnode, storage=storage, **kwargs)

        @staticmethod
        def extrac_all(tree, storage):
            AMDA.ObsDataTreeParser.enter_nodes(tree['dataRoot']['dataCenter'], storage)

    def __init__(self, WSDL='AMDA/public/wsdl/Methods_AMDA.wsdl', server_url="http://amda.irap.omp.eu", inventory_file=None):
        self.METHODS = {
            "REST": AMDA_REST(server_url=server_url),
            "SOAP": AMDA_soap(server_url=server_url, WSDL=WSDL)
        }
        self.parameter = {}
        self.mission = {}
        self.observatory = {}
        self.instrument = {}
        self.dataset = {}
        self.datasetGroup = {}
        self.component = {}
        self.inventory_file = inventory_file
        if inventory_file:
            pathlib.Path(os.path.dirname(inventory_file)).mkdir(parents=True, exist_ok=True)
            if os.path.exists(inventory_file):
                with open(inventory_file, 'r') as f:
                    self._unpack_inventory(jsonpickle.loads(f.read()))

    def _save(self):
        if self.inventory_file:
            with open(self.inventory_file, 'w') as f:
                f.write(jsonpickle.dumps(self._pack_inventory()))

    def __del__(self):
        self._save()

    def _pack_inventory(self):
        return {
            'parameter':    self.parameter,
            'observatory':  self.observatory,
            'instrument':   self.instrument,
            'dataset':      self.dataset,
            'mission':      self.mission,
            'datasetGroup': self.datasetGroup,
            'component':    self.component
        }

    def _unpack_inventory(self, inventory):
        self.__dict__.update(inventory)

    def update_inventory(self, method="SOAP"):
        tree = self.get_obs_data_tree()
        storage = self._pack_inventory()
        AMDA.ObsDataTreeParser.extrac_all(tree, storage)

    def get_token(self, method="SOAP", **kwargs):
        return self.METHODS[method.upper()].get_token()

    def _get_parameter_url(self, start_time, stop_time, parameter_id, method="REST", **kwargs):
        token = self.get_token()
        if type(start_time) is datetime:
            start_time = start_time.isoformat()
        if type(stop_time) is datetime:
            stop_time = stop_time.isoformat()
        url = self.METHODS[method.upper()].get_parameter(
            startTime=start_time, stopTime=stop_time, parameterID=parameter_id, token=token, **kwargs)
        return url

    def _get_header_(self, parameter_id, method="REST", **kwargs):
        r = self.parameter_range(parameter_id)
        url = self._get_parameter_url(r.start_time, r.start_time+timedelta(minutes=1), parameter_id, method, **kwargs)
        print(url)
        with urllib.request.urlopen(url) as data:
            lines = [l for l in data.read().decode().split('\n') if '#' in l]
            return '\n'.join(lines)

    def get_parameter(self, start_time: datetime, stop_time: datetime, parameter_id: str , method: str = "REST", **kwargs) -> Optional[pds.DataFrame]:
        url = self._get_parameter_url(start_time, stop_time, parameter_id, method, **kwargs)
        if url is not None:
            print(url)
            return pds.read_csv(url, delim_whitespace=True, comment='#', parse_dates=True, infer_datetime_format=True,
                                index_col=0, header=None)
        return None

    def get_obs_data_tree(self, method="SOAP") -> dict:
        datatree = xmltodict.parse(requests.get(
            self.METHODS[method.upper()].get_obs_data_tree()).text)
        return datatree

    def parameter_range(self, parameter_id):
        if not len(self.parameter):
            self.update_inventory()
        if parameter_id in self.parameter:
            dataset_name = self.parameter[parameter_id]['dataset']
            if dataset_name in self.dataset:
                dataset = self.dataset[dataset_name]
                print(dataset)
                return DateTimeRange(
                    datetime.strptime(dataset["dataStart"], '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.strptime(dataset["dataStop"],  '%Y-%m-%dT%H:%M:%SZ')
                )


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
        super(CachedAMDA, self).__init__(WSDL, server_url, data_folder+'/amda_inventory.json')
        self.data_folder = data_folder
        self.cache = Cache(data_folder+'/db.json')
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

    def add_to_cache(self, parameter_id: str, dt_range: DateTimeRange , df: pds.DataFrame):
        fname = self.data_folder + '/' + str(uuid.uuid4())
        df.to_pickle(fname)
        self.cache.add_entry(parameter_id, CacheEntry(dt_range, fname))

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
        if type(result) is pds.DataFrame:
            try:
                result = result[start_time:stop_time]
            except:
                print('''can't slice dataframe''')
        return result

    def get_parameter_as_txt(self, start_time, stop_time, parameter_id, method="REST", **kwargs):
        if type(start_time) is str:
            start_time = datetime.fromisoformat(start_time)
        if type(stop_time) is str:
            stop_time = datetime.fromisoformat(stop_time)
        data = self.get_parameter(start_time, stop_time, parameter_id, method, **kwargs)
        header = self.get_header(parameter_id)
        txt = header.format(interval_start=start_time.isoformat(), interval_stop=stop_time.isoformat())+'\n'
        data.index = data.index.format(formatter=lambda x: x.isoformat())
        txt+=data.to_string(index_names=False, header=False,
                       formatters={ i: "{:3.3f}".format for i in range(1,data.shape[1])}
                       )
        return txt

