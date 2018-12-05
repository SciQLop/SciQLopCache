import os
from pathlib import Path
from typing import List, Optional

import jsonpickle
from .datetime_range import DateTimeRange


class CacheEntry:

    dt_range: DateTimeRange
    data_file: str

    __slots__ = ['dt_range', 'data_file']

    def __init__(self, dt_range: DateTimeRange, data_file: str):
        self.dt_range = dt_range
        self.data_file = data_file

    def __eq__(self, other):
        assert type(other) is CacheEntry
        return (self.dt_range == other.dt_range) and (self.data_file == other.data_file)

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
    __slots__ = ['cache_file', '_data']

    def __init__(self, cache_file=None):
        self.cache_file = cache_file or str(Path.home()) + '/.sciqlopcache/db.json'
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as f:
                self._data = jsonpickle.loads(f.read())
        else:
            self._data = {}

    def _save(self):
        with open(self.cache_file, 'w') as f:
            f.write(jsonpickle.dumps(self._data))

    def __del__(self):
        pass

    def __contains__(self, item):
        return item in self._data

    def __getitem__(self, item):
        return self._data[item]

    def add_entry(self, product, entry):
        if product in self._data:
            self._data[product].append(entry)
        else:
            self._data[product] = [entry]

    def get_entries(self, parameter_id: str, dt_range: DateTimeRange) -> List[CacheEntry]:
        if parameter_id in self:
            entries = [entry for entry in self[parameter_id] if dt_range.intersect(entry.dt_range)]
            #return entries if len(entries) else None
            return entries
        else:
            return []

    def get_missing_ranges(self, parameter_id: str, dt_range: DateTimeRange) -> List[DateTimeRange]:
        hit_ranges = self.get_entries(parameter_id, dt_range)
        if hit_ranges:
            return dt_range - hit_ranges
        else:
            return [dt_range]

