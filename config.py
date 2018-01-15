#!/usr/bin/env python
# encoding=utf8

import yaml
import constants


class ConfigService:
    conf_path = constants.conf_path

    with open(conf_path, 'r', encoding="utf-8") as ymlfile:
        instant = yaml.load(ymlfile)

    @classmethod
    def get_instance(cls):
        return cls.instant


