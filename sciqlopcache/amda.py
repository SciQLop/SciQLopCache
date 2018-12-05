import os
import sys
from typing import Optional

import jsonpickle
import requests
from zeep import Client
import pandas as pds
from datetime import datetime, timedelta
import xmltodict
from .cache import Cache, CacheEntry, DateTimeRange
import uuid
import pathlib
import urllib.request

import logging
log = logging.getLogger(__name__)


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
        log.debug(f'REST request {url}')
        if 'success' in r.json():
            if r.json()['success']:
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
            AMDA.ObsDataTreeParser.enter_nodes(tree['dataRoot'], storage)

    def __init__(self, WSDL='AMDA/public/wsdl/Methods_AMDA.wsdl', server_url="http://amda.irap.omp.eu",
                 inventory_file=None):
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
        self.dataCenter = {}
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
            'parameter': self.parameter,
            'observatory': self.observatory,
            'instrument': self.instrument,
            'dataset': self.dataset,
            'mission': self.mission,
            'datasetGroup': self.datasetGroup,
            'component': self.component,
            'dataCenter': self.dataCenter
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
        url = self._get_parameter_url(r.start_time, r.start_time + timedelta(minutes=1), parameter_id, method, **kwargs)
        log.debug(f'Header URL {url}')
        with urllib.request.urlopen(url) as data:
            lines = [l for l in data.read().decode().split('\n') if '#' in l]
            return '\n'.join(lines)

    def get_parameter(self, start_time: datetime, stop_time: datetime, parameter_id: str, method: str = "REST",
                      **kwargs) -> Optional[pds.DataFrame]:
        url = self._get_parameter_url(start_time, stop_time, parameter_id, method, **kwargs)
        if url is not None:
            log.debug(f'Data file URL {url}')
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
                return DateTimeRange(
                    datetime.strptime(dataset["dataStart"], '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.strptime(dataset["dataStop"], '%Y-%m-%dT%H:%M:%SZ')
                )


def extract_header(content: str) -> str:
    lines = content.split('\n')
    for index, _ in enumerate(lines):
        if '# INTERVAL_START' in lines[index]:
            lines[index] = '# INTERVAL_START : {interval_start}'
        if '# INTERVAL_STOP' in lines[index]:
            lines[index] = '# INTERVAL_STOP : {interval_stop}'
    return '\n'.join(lines)

