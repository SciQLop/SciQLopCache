import os
from dataclasses import dataclass
from pathlib import Path
import jsonpickle
import unittest
from datetime import datetime, timedelta

@dataclass
class CacheEntry:
    start_time: datetime
    stop_time: datetime
    data_file: str

    def __contains__(self, item: tuple) -> bool:
        if item[0] > item[1]:
            raise ValueError("Negative time range")
        return (self.start_time <= item[0] <= self.stop_time) or \
               (self.start_time <= item[1] <= self.stop_time)

class Cache:
    def __init__(self, cache_file=None):
        self.cache_file = cache_file or str(Path.home())+'/.sciqlopcache/db.json'
        if os.exists(self.cache_file):
            with open(self.cache_file, 'r') as f:
                self.data = jsonpickle.loads(f.read())
        else :
            self.data = {}

    def __contains__(self, item):
        return item in self.data

    def __getitem__(self, item):
        return self.data[item]

    def __del__(self):
        with open(self.cache_file, 'w') as f:
           f.write(jsonpickle.dumps(self.data))


class _CacheEntryTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_contains(self):
        startdate = datetime(2006, 1, 8, 1, 0, 0)
        stopdate = startdate + timedelta(hours=1)
        entry = CacheEntry(startdate,stopdate,"")
        self.assertTrue([startdate, stopdate] in entry)
        self.assertTrue([startdate+timedelta(minutes=30), stopdate+timedelta(minutes=30)] in entry)
        self.assertTrue([startdate-timedelta(minutes=30), stopdate-timedelta(minutes=30)] in entry)

        self.assertTrue([startdate+timedelta(hours=2), stopdate+timedelta(hours=2)] not in entry)
        with self.assertRaises(ValueError):
            res = [stopdate, startdate] in entry


