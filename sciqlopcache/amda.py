import requests
from zeep import Client
import pandas as pds
from datetime import datetime
import xmltodict
from . import cache

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

    def get_parameter(self, start_time, stop_time, parameter_id, method="SOAP", **kwargs):
        token = self.get_token()
        if type(start_time) is datetime:
            start_time = start_time.isoformat()
        if type(stop_time) is datetime:
            stop_time = stop_time.isoformat()
        url = self.METHODS[method.upper()].get_parameter(
            startTime=start_time, stopTime=stop_time, parameterID=parameter_id, token=token, **kwargs)
        if not url is None:
            print(url)
            return pds.read_csv(url, delim_whitespace=True, comment='#', parse_dates=True, infer_datetime_format=True,
                                index_col=0, header=None)
        return None

    def get_obs_data_tree(self, method="SOAP") -> dict:
        datatree = xmltodict.parse(requests.get(
            self.METHODS[method.upper()].get_obs_data_tree()).text)
        for mission in datatree["dataRoot"]["dataCenter"]["mission"]:
            for instrument in listify(mission["instrument"]):
                for dataset in listify(instrument['dataset']):
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
    lines = content.split()
    for index,_ in enumerate(lines):
        if '# INTERVAL_START' in lines[index]:
            lines[index] = '# INTERVAL_START : {interval_start}'
        if '# INTERVAL_STOP' in lines[index]:
            lines[index] = '# INTERVAL_STOP : {interval_stop}'
    return '\n'.join(lines)


class CachedAMDA(AMDA):
    def __init__(self, WSDL='AMDA/public/wsdl/Methods_AMDA.wsdl', server_url="http://amda.irap.omp.eu", cache_file=None):
        super(self).__init__(WSDL, server_url)
        self.cache = cache.Cache(cache_file)

    def get_parameter(self, start_time, stop_time, parameter_id, method="SOAP", **kwargs):
        if parameter_id in self.cache:
            entries = self.cache[parameter_id]
            
        return super(self).get_parameter(start_time, stop_time, parameter_id, method, **kwargs)

