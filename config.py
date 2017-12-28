#!/usr/bin/env python
# encoding=utf8

import yaml
from datetime import datetime, date


class ConfigService:
    conf_path = "database/configuration.yml"

    with open(conf_path, 'r', encoding="utf-8") as ymlfile:
        instant = yaml.load(ymlfile)

    start_date = str(date.today())

    @classmethod
    def get_instance(cls):
        return cls.instant

    @classmethod
    def get_seeds_file(cls):
        return cls.instant['constants']['seeds_file']

    @classmethod
    def get_new_seeds_file(cls):
        return cls.instant['constants']['new_seeds_file']

    @classmethod
    def get_proxy_db_raw(cls):
        return cls.instant['constants']['proxy_db_raw_file']

    @classmethod
    def get_proxy_db(cls):
        return cls.instant['constants']['proxy_db_file']
