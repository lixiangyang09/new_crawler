#!/usr/bin/env python
# encoding=utf8
import threading


class SyncSet:
    def __init__(self):
        self._data = set()
        self.lock = threading.Lock()

    def __contains__(self, item):
        with self.lock:
            res = item in self._data
        return res

    def exist(self, data):
        """True, already appeared; False, not appeared."""
        with self.lock:
            result = data in self._data
        return result

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
            if self._data:
                return False
            else:
                return True
