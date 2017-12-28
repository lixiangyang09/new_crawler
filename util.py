#!/usr/bin/env python
# encoding=utf8

import hashlib


def get_hash(data):
    code = hashlib.md5(data.encode('utf-8')).hexdigest()
    return code
