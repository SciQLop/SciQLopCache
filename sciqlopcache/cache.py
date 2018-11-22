import os
from dataclasses import dataclass
from pathlib import Path
import jsonpickle
import unittest
from ddt import ddt, data, idata, file_data, unpack
import uuid
from datetime import datetime, timedelta

@dataclass
class DateTimeRange:
    start_time: datetime
    stop_time: datetime

    def intersect(self, other):
        return (self.stop_time >= other[0] ) and (self.start_time <= other[1])

    def __getitem__(self, item):
        return self.start_time if item==0 else self.stop_time

    def __contains__(self, item: object) -> bool:
        if item[0] > item[1]:
            raise ValueError("Negative time range")
        return (self.start_time <= item[0] <= self.stop_time) or \
               (self.start_time <= item[1] <= self.stop_time)

    def __add__(self, other):
        if type(other) is timedelta:
            return DateTimeRange(self.start_time + other, self.stop_time + other)
        else:
            raise TypeError()

    def __sub__(self, other):
        if type(other) is timedelta:
            return DateTimeRange(self.start_time - other, self.stop_time - other)
        elif type(other) is DateTimeRange:
            res = []
            if not self.intersect(other):
                res = [DateTimeRange(self.start_time, self.stop_time)]
            else:
                if self.start_time < other[0]:
                    res.append(DateTimeRange(self.start_time, other[0]))
                if self.stop_time > other[1]:
                    res.append(DateTimeRange(other[1], self.stop_time))
            return res
        else:
            raise TypeError()



@dataclass
class CacheEntry:
    dt_range: DateTimeRange
    data_file: str

    def __contains__(self, item: object) -> bool:
        return item in self.dt_range

    def __sub__(self, other):
        if type(other) is tuple:
            if other in self:
                pass


class Cache:
    def __init__(self, cache_file=None):
        self.cache_file = cache_file or str(Path.home())+'/.sciqlopcache/db.json'
        if os.path.exists(self.cache_file):
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

    def add_entry(self, product, entry):
        if product in self.data:
            self.data[product].append(entry)
        else:
            self.data[product] = [entry]

    def get_missing_ranges(self, parameter_id: str, dt_range: DateTimeRange) -> list:
        missing_ranges = []
        if parameter_id in self:
            entries = self[parameter_id]
            overlap = [entry for entry in entries if dt_range in entry]
            missing_ranges = [item for entry in overlap for item in (dt_range - entry.dt_range)]
        else:
            return [DateTimeRange]
        return missing_ranges


class _CacheEntryTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_contains(self):
        start_date = datetime(2006, 1, 8, 1, 0, 0)
        stop_date = start_date + timedelta(hours=1)
        dt_range = DateTimeRange(start_date, stop_date)
        entry = CacheEntry(dt_range, "")
        self.assertTrue((start_date, stop_date) in entry)
        self.assertTrue((start_date + timedelta(minutes=30), stop_date + timedelta(minutes=30)) in entry)
        self.assertTrue((start_date - timedelta(minutes=30), stop_date - timedelta(minutes=30)) in entry)

        self.assertTrue([start_date+timedelta(hours=2), stop_date+timedelta(hours=2)] not in entry)
        with self.assertRaises(ValueError):
            res = (stop_date, start_date) in entry


class _CacheTest(unittest.TestCase):
    def setUp(self):
        self.dbfile = str(uuid.uuid4())
        self.cache = Cache(self.dbfile)
        start_date = datetime(2006, 1, 8, 1, 0, 0)
        stop_date = start_date + timedelta(hours=1)
        dt_range = DateTimeRange(start_date, stop_date)
        for i in range(10):
            self.cache.add_entry('product1', CacheEntry(dt_range, ""))
            dt_range += timedelta(days=1)

    def tearDown(self):
        del self.cache
        os.remove(self.dbfile)

    def test_get_missing_ranges(self):
        dt_range = DateTimeRange(datetime(2006, 1, 8, 1, 0, 0), datetime(2006, 1, 8, 2, 0, 0))
        missing = self.cache.get_missing_ranges('product1', dt_range)
        print(missing)


@ddt
class _DateTimeRangeTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @data(
        (
                DateTimeRange(datetime(2006, 1, 8, 1, 0, 0), datetime(2006, 1, 8, 2, 0, 0)),
                DateTimeRange(datetime(2006, 1, 8, 0, 0, 0), datetime(2006, 1, 8, 3, 0, 0)),
                []
        ),
        (
                DateTimeRange(datetime(2006, 1, 8, 1, 0, 0), datetime(2006, 1, 8, 2, 0, 0)),
                DateTimeRange(datetime(2006, 1, 8, 1, 0, 0), datetime(2006, 1, 8, 2, 0, 0)),
                []
         ),
        (
                DateTimeRange(datetime(2006, 1, 8, 0, 0, 0), datetime(2006, 1, 8, 4, 0, 0)),
                DateTimeRange(datetime(2006, 1, 8, 1, 0, 0), datetime(2006, 1, 8, 2, 0, 0)),
                [
                    DateTimeRange(datetime(2006, 1, 8, 0, 0, 0), datetime(2006, 1, 8, 1, 0, 0)),
                    DateTimeRange(datetime(2006, 1, 8, 2, 0, 0), datetime(2006, 1, 8, 4, 0, 0)),
                ]
        ),
        (
                DateTimeRange(datetime(2006, 1, 8, 0, 0, 0), datetime(2006, 1, 8, 4, 0, 0)),
                DateTimeRange(datetime(2006, 1, 8, 3, 0, 0), datetime(2006, 1, 8, 5, 0, 0)),
                [
                    DateTimeRange(datetime(2006, 1, 8, 0, 0, 0), datetime(2006, 1, 8, 3, 0, 0))
                ]
        ),
        (
                DateTimeRange(datetime(2006, 1, 8, 2, 0, 0), datetime(2006, 1, 8, 4, 0, 0)),
                DateTimeRange(datetime(2006, 1, 8, 0, 0, 0), datetime(2006, 1, 8, 3, 0, 0)),
                [
                    DateTimeRange(datetime(2006, 1, 8, 3, 0, 0), datetime(2006, 1, 8, 4, 0, 0))
                ]
        )
    )
    @unpack
    def test_range_diff(self, range1, range2, expected):
        self.assertTrue(range1-range2 == expected)
