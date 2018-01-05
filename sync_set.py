#!/usr/bin/env python
# encoding=utf8
import threading


class SyncSet:
    def __init__(self):
        self._data = set()
        self.lock = threading.Lock()

    def __contains__(self, item):
        with self.lock:
            return item in self._data

    def __delitem__(self, data):
        with self.lock:
            if data in self._data:
                self._data.remove(data)

    def __nonzero__(self):
        with self.lock:
            return bool(self._data)

    def exist(self, data):
        """True, already appeared; False, not appeared."""
        with self.lock:
            return data in self._data

    def add(self, data):
        with self.lock:
            self._data.add(data)

    def remove(self, data):
        with self.lock:
            if data in self._data:
                self._data.remove(data)

    def get(self):
        res = None
        with self.lock:
            for v in self._data:
                self._data.remove(v)
                res = v
                break
        return res

    def empty(self):
        with self.lock:
            return bool(self._data)
