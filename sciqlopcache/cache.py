import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Any

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
        return ((self.stop_time >= other[0] ) and (self.start_time <= other[1])) or (other[0] <= self.start_time <= other[1]) or (other[0] <= self.stop_time <= other[1])

    def __repr__(self):
        return str(self.start_time.isoformat() +"->"+ self.stop_time.isoformat())

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
        elif hasattr(other,'start_time') and  hasattr(other,'stop_time'):
            res = []
            if not self.intersect(other):
                res = [DateTimeRange(self.start_time, self.stop_time)]
            else:
                if self.start_time < other[0]:
                    res.append(DateTimeRange(self.start_time, other[0]))
                if self.stop_time > other[1]:
                    res.append(DateTimeRange(other[1], self.stop_time))
            return res
        elif type(other) is list:
            diff = []
            if len(other) > 1:
                other.sort()
                left = (DateTimeRange(self.start_time, other[0].stop_time) - other[0])
                if left:
                    diff += left
                diff += [
                    DateTimeRange(pair[0].stop_time, pair[1].start_time)
                    for pair in zip(other[0:-1], other[1:])
                         ]
                right = (DateTimeRange(other[-1].start_time, self.stop_time) - other[-1])
                if right:
                    diff += right
            elif len(other):
                diff += (self - other[0])
            else:
                return [self]
            return diff
        else:
            raise TypeError()

    def __lt__(self, other):
        return self.start_time < other.start_time

    def __gt__(self, other):
        return self.start_time > other.start_time


@dataclass
class CacheEntry:
    dt_range: DateTimeRange
    data_file: str

    @property
    def start_time(self):
        return self.dt_range.start_time

    @property
    def stop_time(self):
        return self.dt_range.stop_time

    def __getitem__(self, item):
        return self.start_time if item==0 else self.stop_time

    def __contains__(self, item: object) -> bool:
        return item in self.dt_range

    def __lt__(self, other):
        return self.start_time < other.start_time

    def __gt__(self, other):
        return self.start_time > other.start_time


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

    def get_missing_ranges(self, parameter_id: str, dt_range: DateTimeRange) -> List[DateTimeRange]:
        if parameter_id in self:
            entries = self[parameter_id]
            hit_ranges = [entry for entry in entries if dt_range.intersect(entry.dt_range)]
            return dt_range - hit_ranges
        else:
            return [dt_range]


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

@ddt
class _CacheTest(unittest.TestCase):
    def setUp(self):
        self.dbfile = str(uuid.uuid4())
        self.cache = Cache(self.dbfile)
        start_date = datetime(2006, 1, 8, 0, 0, 0)
        stop_date  = datetime(2006, 1, 8, 1, 0, 0)
        dt_range = DateTimeRange(start_date, stop_date)
        for i in range(10):
            self.cache.add_entry('product1', CacheEntry(dt_range, ""))
            dt_range += timedelta(days=1)

    @data(
        (
                'product1',
                DateTimeRange(datetime(2006, 1, 8, 0, 20, 0), datetime(2006, 1, 8, 0, 40, 0)),
                []
        ),
        (
                'product not in cache',
                DateTimeRange(datetime(2006, 1, 8, 0, 20, 0), datetime(2006, 1, 8, 0, 40, 0)),
                [
                    DateTimeRange(datetime(2006, 1, 8, 0, 20, 0), datetime(2006, 1, 8, 0, 40, 0))
                ]
        ),
        (
                'product1',
                DateTimeRange(datetime(2016, 1, 8, 0, 20, 0), datetime(2016, 1, 8, 0, 40, 0)),
                [
                    DateTimeRange(datetime(2016, 1, 8, 0, 20, 0), datetime(2016, 1, 8, 0, 40, 0))
                ]
        ),
        (
                'product1',
                DateTimeRange(datetime(2006, 1, 8, 0, 20, 0), datetime(2006, 1, 8, 1, 40, 0)),
                [
                    DateTimeRange(datetime(2006, 1, 8, 1, 0, 0), datetime(2006, 1, 8, 1, 40, 0))
                ]
        ),
        (
                'product1',
                DateTimeRange(datetime(2006, 1, 7, 23, 20, 0), datetime(2006, 1, 8, 1, 0, 0)),
                [
                    DateTimeRange(datetime(2006, 1, 7, 23, 20, 0), datetime(2006, 1, 8, 0, 0, 0))
                ]
        ),
        (
                'product1',
                DateTimeRange(datetime(2006, 1, 7, 23, 20, 0), datetime(2006, 1, 8, 1, 40, 0)),
                [
                    DateTimeRange(datetime(2006, 1, 7, 23, 20, 0), datetime(2006, 1, 8, 0, 0, 0)),
                    DateTimeRange(datetime(2006, 1, 8, 1, 0, 0), datetime(2006, 1, 8, 1, 40, 0))
                ]
        ),
        (
                'product1',
                DateTimeRange(datetime(2006, 1, 8, 0, 0, 0), datetime(2006, 1, 9, 1, 40, 0)),
                [
                    DateTimeRange(datetime(2006, 1, 8, 1, 0, 0), datetime(2006, 1, 9, 0, 0, 0)),
                    DateTimeRange(datetime(2006, 1, 9, 1, 0, 0), datetime(2006, 1, 9, 1, 40, 0))
                ]
        ),
    )
    @unpack
    def test_get_missing_ranges(self, product, dt_range, expected):
        missing = self.cache.get_missing_ranges(product, dt_range)
        self.assertEqual(missing, expected)

    def tearDown(self):
        del self.cache
        os.remove(self.dbfile)


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
        self.assertEquals(range1-range2, expected)

    def test_range_substract_timedelta(self):
        self.assertEquals(
            DateTimeRange(datetime(2006, 1, 8, 1, 0, 0), datetime(2006, 1, 8, 2, 0, 0))
            -
            timedelta(hours=1),
            DateTimeRange(datetime(2006, 1, 8, 0, 0, 0), datetime(2006, 1, 8, 1, 0, 0)))

    def test_add_with_wrong_type(self):
        with self.assertRaises(TypeError):
            DateTimeRange(datetime(2006, 1, 8, 3, 0, 0), datetime(2006, 1, 8, 4, 0, 0)) + 1

    def test_substract_with_wrong_type(self):
        with self.assertRaises(TypeError):
            DateTimeRange(datetime(2006, 1, 8, 3, 0, 0), datetime(2006, 1, 8, 4, 0, 0)) - 1
