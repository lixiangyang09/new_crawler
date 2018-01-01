#!/usr/bin/env python
# encoding=utf8

import shutil
import codecs
import logging
import util
import os
import tarfile
import re

logger = logging.getLogger("StoreService")


class RedisService:

    @classmethod
    def send_msg(cls, data):
        pass


class FileService:

    @classmethod
    def pack_folder(cls, tar_file_name_with_path, target_folder, delete_folder=False):
        with tarfile.open(tar_file_name_with_path, 'w:gz') as tar:
            tar.add(target_folder)
        if delete_folder:
            shutil.rmtree(target_folder)  # delete the folder

    @classmethod
    def unpack_file(cls, tar_file_path, target_path, delete_file=False):
        with tarfile.open(tar_file_path, 'r:gz') as tar:
            tar.extractall(target_path)
        if delete_file:
            os.remove(tar_file_path)

    @classmethod
    def save_file(cls, path, file_name, data, encoding="utf-8"):
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
    def get_latest_data_file(cls, path):
        logger.info("get_latest_data_file")
        files = os.listdir(path)
        # 2017-10-09_10_00_01.810061_123.tar.gz
        data_file_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})_(\d{2})_(\d{2})_(\d{2}).(\d{6})(.*).tar.gz$')
        data_files = [path + "/" + x for x in files if data_file_pattern.match(x)]
        return max(data_files)

    @classmethod
    def save_data(cls, data):
        cls.save_file(util.get_output_data_dir(), str(util.get_uuid()), data)
        RedisService.send_msg(data)
