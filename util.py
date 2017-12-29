#!/usr/bin/env python
# encoding=utf8

import hashlib
import uuid
from datetime import datetime, date
import os

start_date = str(date.today())


def get_hash(data):
    code = hashlib.md5(data.encode('utf-8')).hexdigest()
    return code


def get_uuid():
    return uuid.uuid1()


def get_output_base_dir():
    return 'output'


def get_output_data_dir():
    return get_output_base_dir() + "/" + start_date


