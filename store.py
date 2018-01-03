#!/usr/bin/env python
# encoding=utf8

import shutil
import codecs
import logging
import util
import os
import tarfile
import re
import oss2
from futile.queue.redis_queue import QueueProducer, QueueConsumer


logger = logging.getLogger("StoreService")


class RedisService:

    topic = "lxy_test"

    producer = QueueProducer('10.1.1.3', '6379')
    consumer = QueueConsumer('10.1.1.3', '6379', topic, 'test')

    @classmethod
    def send_msg(cls, data):
        cls.producer.send_events(cls.topic, [data])

    @classmethod
    def receive_msg(cls):
        events = cls.consumer.recv_events(10)
        return events


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
        if not os.path.exists(path):
            os.makedirs(path)
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


class OSSClient:
    def __init__(self, bucket_name, prefix, sep):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.sep = sep
        self.access_key_id = "LTAIOurI3zqA5Nbd"
        self.access_key_secret = "593emZqhErT8cTlUXpOAkGt4wAxKrj"
        self.endpoint = "oss-cn-beijing.aliyuncs.com"
        self.auth = oss2.Auth(self.access_key_id, self.access_key_secret)
        self.bucket_ins = oss2.Bucket(self.auth, self.endpoint, self.bucket_name)

    def _gen_remote_file_name(self, file):
        file_name = os.path.basename(file)
        return f"{self.prefix}{self.sep}{file_name}"

    def _gen_local_file_name(self, output_base, remote_file):
        ind_start = remote_file.find(self.sep)
        file_name = remote_file[ind_start:]
        folder = output_base + "/"
        if not os.path.exists(folder):
            os.makedirs(folder)
        return folder + "/" + file_name

    def get_file_list(self):
        return [obj.key for obj in oss2.ObjectIterator(self.bucket_ins, prefix=self.prefix)]

    def put_file(self, local_file, remote_file):
        with open(local_file, 'rb') as file_obj:
            self.bucket_ins.put_object(remote_file, file_obj)

    def _put_files(self, file_set):
        # generate remote file name
        if isinstance(file_set, list):
            for file in file_set:
                self.put_file(file, self._gen_remote_file_name(file))
        elif isinstance(file_set, dict):
            for local, remote in file_set.items():
                self.put_file(local, remote)

    def get_file(self, remote_file, local_file):
        self.bucket_ins.get_object_to_file(remote_file, local_file)

    def get_all_files(self, output_base):
        file_list = self.get_file_list()
        for file in file_list:
            # local_name = self._gen_local_file_name(output_base, file)
            print(f"Downloading file {file}")
            file_name = os.path.basename(file)
            dir_name = os.path.dirname(file)
            local_output_dir = output_base + "/" + dir_name
            if not os.path.exists(local_output_dir):
                os.makedirs(local_output_dir)
            self.get_file(file, local_output_dir + "/" + file_name)

    def upload_file(self, file):
        if isinstance(file, str):
            files = [file]
        else:
            files = file

        upload_success = False
        try:
            for _ in range(3):
                self._put_files(files)
                upload_success = True
                break
        except Exception as e:
            logger.warning(f"Upload to oss exception!")
            logger.info(e.__traceback__)
        if upload_success:
            logger.info("upload to oss successfully.")
        else:
            logger.warning("failed to upload data to oss")


if __name__ == '__main__':
    test_data = {'a': 1, 'b': 2}
    RedisService.send_msg(test_data)
    msgs = RedisService.receive_msg()
    print(msgs)
