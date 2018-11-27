import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import jsonpickle
from datetime import datetime, timedelta


@dataclass
class DateTimeRange:
    start_time: datetime
    stop_time: datetime

    def intersect(self, other):
        return ((self.stop_time >= other[0]) and (self.start_time <= other[1])) or (
                    other[0] <= self.start_time <= other[1]) or (other[0] <= self.stop_time <= other[1])

    def __repr__(self):
        return str(self.start_time.isoformat() + "->" + self.stop_time.isoformat())

    def __getitem__(self, item):
        return self.start_time if item == 0 else self.stop_time

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
        elif hasattr(other, 'start_time') and hasattr(other, 'stop_time'):
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
        return self.start_time if item == 0 else self.stop_time

    def __contains__(self, item: object) -> bool:
        return item in self.dt_range

    def __lt__(self, other):
        return self.start_time < other.start_time

    def __gt__(self, other):
        return self.start_time > other.start_time


class Cache:
    def __init__(self, cache_file=None):
        self.cache_file = cache_file or str(Path.home()) + '/.sciqlopcache/db.json'
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as f:
                self.data = jsonpickle.loads(f.read())
        else:
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

    def get_entries(self, parameter_id: str, dt_range: DateTimeRange) -> Optional[List[CacheEntry]]:
        if parameter_id in self:
            entries = [entry for entry in self[parameter_id] if dt_range.intersect(entry.dt_range)]
            return entries if len(entries) else None
        else:
            return None

    def get_missing_ranges(self, parameter_id: str, dt_range: DateTimeRange) -> List[DateTimeRange]:
        hit_ranges = self.get_entries(parameter_id, dt_range)
        if hit_ranges:
            return dt_range - hit_ranges
        else:
            return [dt_range]

