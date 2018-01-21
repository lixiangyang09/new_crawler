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
from futile.queue.redis_queue import get_redis_client, QueueProducer, QueueConsumer

logger = logging.getLogger("StoreService")


class RedisService:

    topic = "crawl_house"
    redis_client = get_redis_client('10.1.1.3')
    producer = QueueProducer(redis_client)
    consumer = QueueConsumer(redis_client, topic, 'test')

    @classmethod
    def send_msg(cls, data):
        cls.producer.send_events(cls.topic, [data])

    @classmethod
    def receive_msg(cls):
        events = cls.consumer.recv_events(10)
        return events


class FileService:

    @classmethod
    def pack_files(cls, tar_file_name_with_path, target_files, delete_after_pack=False):
        file_path = os.path.dirname(tar_file_name_with_path)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        with tarfile.open(tar_file_name_with_path, 'w:gz') as tar:
            current_dir = os.getcwd()
            for file, path in target_files:
                os.chdir(path)
                tar.add(file)
                os.chdir(current_dir)

                full_path = path + '/' + file
                if delete_after_pack:
                    if os.path.isdir(full_path):
                        shutil.rmtree(full_path)  # delete the folder
                    else:
                        if os.path.isfile(full_path):
                            os.remove(full_path)

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
        self.retry_count = 30

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

    def upload_file(self, local_file, remote_file=''):
        logger.info(f"Beging to upload {local_file} to oss.")
        upload_success = False
        try:
            for _ in range(self.retry_count):
                if not remote_file:
                    remote_file = self._gen_remote_file_name(local_file)
                self.put_file(local_file, remote_file)
                upload_success = self.check_file_consistency(local_file, remote_file)
                if upload_success:
                    break
        except Exception as e:
            logger.warning(f"Upload to oss exception!")
            logger.info(e.__traceback__)
        if upload_success:
            logger.info("upload to oss successfully.")
        else:
            logger.warning(f"failed to upload {local_file} to oss after {self.retry_count}times")

    def check_file_consistency(self, local, remote):
        tmp_check_dir = 'check_data_file'
        if os.path.exists(tmp_check_dir):
            shutil.rmtree(tmp_check_dir)
        os.makedirs(tmp_check_dir)

        local_dir = tmp_check_dir + '/' + 'local'
        os.mkdir(local_dir)
        remote_dir = tmp_check_dir + '/remote'
        os.mkdir(remote_dir)

        local_file = tmp_check_dir + '/local.tar.gz'
        remote_file = tmp_check_dir + '/remote.tar.gz'

        shutil.copy(local, local_file)
        self.get_file(remote, remote_file)

        # check the md5 first
        local_md5 = util.get_file_md5(local_file)
        remote_md5 = util.get_file_md5(remote_file)
        if not local_md5 == remote_md5:
            logger.warning('tar file md5 not same')
            return False

        # check file size
        if not os.path.getsize(local_file) == os.path.getsize(remote_file):
            logger.warning('tar file size not equal')
            return False

        FileService.unpack_file(local_file, local_dir)
        FileService.unpack_file(remote_file, remote_dir)

        local_contents = []
        remote_contents = []
        for root, directories, filenames in os.walk(local_dir):
            for filename in filenames:
                local_contents.append(os.path.join(root, filename))
        for root, directories, filenames in os.walk(remote_dir):
            for filename in filenames:
                remote_contents.append(os.path.join(root, filename))

        if not len(local_contents) == len(remote_contents):
            logger.warning('File count not equal')
            return False

        for file in local_contents:
            remote_file = file.replace('local', 'remote')
            if remote_file in remote_contents:
                if os.path.getsize(remote_file) == os.path.getsize(remote_file):
                    pass
                else:
                    logger.warning(f'{file} not equal {remote_file}')
            else:
                logger.warning(f'Not found file {remote_file}')
        if os.path.exists(tmp_check_dir):
            shutil.rmtree(tmp_check_dir)
        logger.info(f"File upload successfully after check.")
        return True


if __name__ == '__main__':
    test_data = {'a': 1, 'b': 2}
    RedisService.send_msg(test_data)
    msgs = RedisService.receive_msg()
    print(msgs)
    # oss_client = OSSClient("buaacraft", "lxy", "/")
    # oss_client.upload_file('./report_data/2017-12-30_report_data.tar.gz')
