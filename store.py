#!/usr/bin/env python
# encoding=utf8

import shutil
import codecs
import logging
import util
import os

logger = logging.getLogger("StoreService")


class RedisService:

    @classmethod
    def send_msg(cls, data):
        pass


class StoreService:

    @classmethod
    def save_file(cls, path, file_name, data, encoding="utf-8"):
        if not os.path.exists(path):
            os.makedirs(path)
        logger.info(f"save file {path}/{file_name}")
        with codecs.open(path + "/" + file_name, 'w', encoding) as f:
            f.write(str(data))

    @classmethod
    def load_file(cls, input_file, decoding="utf-8"):
        with codecs.open(input_file, 'r', decoding) as f:
            index_content = f.readlines()
            data = "\n".join(index_content)
        return data

    @classmethod
    def save_data(cls, data):
        cls.save_file(util.get_output_data_dir(), str(util.get_uuid()), data)
        RedisService.send_msg(data)
