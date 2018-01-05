#!/usr/bin/env python
# encoding=utf8

import os
import pickle
import logging
import shutil
import util
from sync_set import SyncSet


class MarkDict:
    def __init__(self):
        self._data = dict()

    def add(self, hash_code, hash_date):
        self._data[hash_code] = hash_date

    def __contains__(self, hash_code):
        if hash_code in self._data:
            return True
        else:
            return False

    def house_keep(self):
        pass
        # now = datetime.now()
        # to_delete = []
        # for hash_code, date in self._data.items():
        #     hash_date = datetime.strptime(date, '%Y-%m-%d')
        #     if (now - hash_date) > timedelta(days=30):
        #         to_delete.append(self._data[hash_code])
        # for hash_code in to_delete:
        #     del self._data[hash_code]


class CacheService:
    logger = logging.getLogger(__name__)
    base_dir = 'cache_data'
    cache_time = '2012-01-01'

    house_cache_file = 'house_cache'
    daily_cache_file = 'daily_cache'
    down_cache_file = 'down_cache'
    seed_cache_file = 'seed_cache'

    house_cache_data = dict()
    daily_cache_data = dict()
    down_cache_data = MarkDict()
    current_seeds = SyncSet()
    new_seeds = SyncSet()
    to_delete_seeds = SyncSet()

    seed_file_list = []

    @classmethod
    def reset(cls):
        cls.new_seeds = SyncSet()
        cls.to_delete_seeds = SyncSet()

    @classmethod
    def _load_cache(cls, file_name):
        cls.logger.info(f"load cache of {file_name}")

        file_full_path = cls.base_dir + '/' + file_name

        if os.path.exists(file_full_path):
            with open(file_full_path, "rb") as f:
                cache_tmp = pickle.load(f)
            time = cache_tmp['time']
            data = cache_tmp['data']
            cls.logger.info(f"Finish load cache file {file_full_path} with {time}")
            return time, data
        else:
            cls.logger.warning(f"Can't find cache file {file_full_path}")
            return '', None

    @classmethod
    def start(cls):
        # check cache base dir
        if not os.path.exists(cls.base_dir):
            os.mkdir(cls.base_dir)
        # load cache
        house_time, cls.house_cache_data = cls._load_cache(cls.house_cache_file)
        daily_time, cls.daily_cache_data = cls._load_cache(cls.daily_cache_file)
        down_time, cls.down_cache_data = cls._load_cache(cls.down_cache_file)
        seed_time, cls.seed_cache_data = cls._load_cache(cls.seed_cache_file)

        if house_time and daily_time and down_time and seed_time \
                and house_time == daily_time and daily_time == seed_time and seed_time == house_time:
            cls.cache_time = house_time
            cls.logger.info(f"Calculate statistics with cache of {cls.cache_time}.")
        else:
            cls.logger.warning(f"Calculate statistics without cache.")
            cls.house_cache_data = dict()
            cls.daily_cache_data = dict()
            cls.down_cache_data = MarkDict()
            cls.seed_cache_data = SyncSet()

    @classmethod
    def _save_cache(cls, file, data, latest_folder_time):
        file_full_path = cls.base_dir + '/' + file
        cls.logger.info(f"Saving cache file {file_full_path}")
        with open(file_full_path, "wb") as f:
            pickle.dump(dict(time=latest_folder_time, data=data), f)
        shutil.copy(file_full_path, file_full_path + '_' + latest_folder_time)
        cls.logger.info(f"Finish saving cache file {file_full_path} with time {latest_folder_time}")

    @classmethod
    def stop(cls, latest_folder_time):
        cls.logger.info(f"CacheService is stopping with saving cache of {latest_folder_time}.")
        cls.fresh_cache()
        cls._save_cache(cls.house_cache_file, cls.house_cache_data, latest_folder_time)
        cls._save_cache(cls.down_cache_file, cls.down_cache_data, latest_folder_time)
        cls._save_cache(cls.daily_cache_file, cls.daily_cache_data, latest_folder_time)
        cls._save_cache(cls.seed_cache_file, cls.seed_cache_data, latest_folder_time)
        cls.generate_seed_file(cls.base_dir + '/seeds_file_auto_save' + '_' + latest_folder_time)

    @classmethod
    def generate_new_seed_cache(cls, new_seed_file):
        """
        Generate the true new seeds based on the current cache and the fake new seed file
        :param new_seed_file:
        :return: new seeds hash list
        """
        cls.seed_file_list.append(new_seed_file)
        res = SyncSet()
        with open(new_seed_file) as f:
            for line in f:
                hash_code = util.get_line_hash(line)
                if hash_code:
                    res.add(hash_code)
        return res

    @classmethod
    def load_seed_file(cls, seed_file):
        """
        Load the seed hash from seed_file to current_seeds
        :param seed_file:
        :return:
        """
        cls.seed_file_list.append(seed_file)
        cls.logger.info(f"Loading seed file {seed_file}")
        with open(seed_file) as f:
            for line in f:
                hash_code = util.get_line_hash(line)
                if hash_code:
                    cls.current_seeds.add(hash_code)

    @classmethod
    def fresh_cache(cls):
        """
        Fresh the cache based on the to_delete data
        :return:
        """
        cls.logger.info(f"Fresh cache based on the to_delete data")
        while True:
            delete_hash = cls.to_delete_seeds.get()
            if delete_hash:
                del cls.current_seeds[delete_hash]
                if delete_hash in cls.house_cache_data:
                    del cls.house_cache_data[delete_hash]
            else:
                break

    @classmethod
    def generate_seed_file(cls, result_seed_file):
        output_count = 0
        with open(result_seed_file, 'w') as output:
            for file in cls.seed_file_list:
                with open(file) as f:
                    for line in f:
                        hash_code = util.get_line_hash(line)
                        if hash_code:
                            output.write(line)
                            output_count += 1
        cls.logger.info(f"Finish generate seed file {result_seed_file} with {output_count} lines.")

    @classmethod
    def get_house(cls, hash_code):
        return cls.house_cache_data.get(hash_code, None)

    @classmethod
    def update_house(cls, house):
        cls.house_cache_data[house['hash_code']] = house

    @classmethod
    def already_off_shelf(cls, hash_code, date):
        if hash_code in cls.down_cache_data:
            res = True
        else:
            cls.to_delete_seeds.add(hash_code)
            cls.down_cache_data.add(hash_code, date)
            res = False
        return res

    @classmethod
    def update_daily_cache(cls, date, data):
        cls.daily_cache_data[date] = data
