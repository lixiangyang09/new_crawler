#!/usr/bin/env python
# encoding=utf8

import os
import pickle
import logging
import shutil
import util
import copy
from config import ConfigService
import constants


class BasicStatistic:
    def __init__(self):
        self.total = 0
        self.up = 0
        self.down = 0
        self.inc = 0
        self.dec = 0

    def reset(self):
        self.total = 0
        self.up = 0
        self.down = 0
        self.inc = 0
        self.dec = 0

    def __iadd__(self, ins):
        if isinstance(ins, BasicStatistic):
            self.total += ins.total
            self.up += ins.up
            self.down += ins.down
            self.inc += ins.inc
            self.dec += ins.dec
        else:
            print(f"+= . Not an instance of BasicStatistic.")
        return self

    def __repr__(self):
        return f"在售, {self.total}, " \
               f"涨价, {self.inc}, " \
               f"降价, {self.dec}, " \
               f"上架, {self.up}, " \
               f"下架, {self.down}"


class CacheService:
    logger = logging.getLogger(__name__)
    base_dir = 'cache_data'
    cache_date = '2012-01-01'
    data_start_date = ''
    house_cache_file = 'house_cache'
    daily_cache_file = 'daily_cache'

    house_cache_data = dict()
    daily_cache_data = dict()

    to_delete_seeds = set()

    @classmethod
    def _load_cache(cls, file_name):
        cls.logger.info(f"load cache of {file_name}")

        file_full_path = cls.base_dir + '/' + file_name

        if os.path.exists(file_full_path):
            with open(file_full_path, "rb") as f:
                cache_tmp = pickle.load(f)
            date = cache_tmp['date']
            data = cache_tmp['data']
            cls.data_start_date = cache_tmp['start']
            cls.logger.info(f"Finish load cache file {file_full_path} with {date}")
            return date, data
        else:
            cls.logger.warning(f"Can't find cache file {file_full_path}")
            return '', None

    @classmethod
    def start(cls):
        # check cache base dir
        if not os.path.exists(cls.base_dir):
            os.mkdir(cls.base_dir)
        # load cache
        house_date, cls.house_cache_data = cls._load_cache(cls.house_cache_file)
        daily_date, cls.daily_cache_data = cls._load_cache(cls.daily_cache_file)

        if house_date and daily_date and house_date == daily_date:
            cls.cache_date = house_date
            cls.logger.info(f"Calculate statistics with cache of {cls.cache_date}.")
        else:
            cls.logger.warning(f"Calculate statistics without cache.")
            cls.house_cache_data = dict()
            cls.daily_cache_data = dict()

    @classmethod
    def _save_cache(cls, file, data, latest_folder_date):
        file_full_path = cls.base_dir + '/' + file
        cls.logger.info(f"Saving cache file {file_full_path}")
        with open(file_full_path, "wb") as f:
            pickle.dump(dict(date=latest_folder_date, data=data, start=cls.data_start_date), f)
        shutil.copy(file_full_path, file_full_path + '_' + latest_folder_date)
        cls.logger.info(f"Finish saving cache file {file_full_path} with date {latest_folder_date}")

    @classmethod
    def stop(cls, latest_folder_date):
        cls.logger.info(f"CacheService is stopping with saving cache of {latest_folder_date}.")
        cls._save_cache(cls.house_cache_file, cls.house_cache_data, latest_folder_date)
        cls._save_cache(cls.daily_cache_file, cls.daily_cache_data, latest_folder_date)

        cls.generate_chart_data()

    @classmethod
    def fresh_seed_file(cls, seed_file):
        """
        Fresh the seed file based on the to delete
        :param seed_file:
        :return:
        """
        cls.logger.info(f"Fresh seed file from {seed_file} to {constants.seeds_file}")
        with open(constants.seeds_file, 'w') as output_handle:
            with open(seed_file) as f:
                for line in f:
                    hash_code = util.get_line_hash(line)
                    if hash_code and hash_code not in cls.to_delete_seeds:
                        output_handle.write(line)

    @classmethod
    def get_house(cls, hash_code):
        return cls.house_cache_data.get(hash_code, None)

    @classmethod
    def get_daily_data(cls, date=''):
        if date:
            return cls.daily_cache_data[date]
        else:
            return cls.daily_cache_data

    @classmethod
    def update_house(cls, house):
        cls.house_cache_data[house['hash_code']] = house

    @classmethod
    def generate_chart_data(cls):
        pass

    @classmethod
    def _update_daily_data(cls, date, city, dis, key):
        template = {'total': 0, 'up': 0, 'down': 0, 'inc': 0, 'dec': 0}
        daily_data = cls.daily_cache_data.get(date, dict())
        city_data = daily_data.get(city, dict())
        dis_data = city_data.get(dis, copy.deepcopy(template))

        dis_data[key] += 1

        city_data[dis] = dis_data
        daily_data[city] = city_data
        cls.daily_cache_data[date] = daily_data

    @classmethod
    def update_daily_data(cls, key, date, data):
        if date < cls.data_start_date:
            return
        cls._update_daily_data(date, data['city'], data['district'], key)
        # cls._update_daily_data(date, data['city'], 'total', key)

    @classmethod
    def assume_start_date(cls, date):
        if cls.data_start_date == '':
            cls.data_start_date = date

    # @classmethod
    # def _dict_append_data(cls, base, name, input_data, date):
    #     base[name]['x_data'].append(date)
    #     base[name]['on'].append(input_data.total)
    #     base[name]['up'].append(input_data.up)
    #     base[name]['down'].append(input_data.down)
    #     base[name]['inc'].append(input_data.inc)
    #     base[name]['dec'].append(input_data.dec)
    #
    # @classmethod
    # def update_daily_cache(cls, date, data):
    #     template = {'x_data': [], 'on': [], 'up': [], 'down': [], 'inc': [], 'dec': []}
    #     cls.daily_cache_data[date] = data
    #     for city, city_dis in data.items():
    #         city_cache = cls.daily_cache_data.get(city, dict())
    #         if 'total' not in city_cache:
    #             city_cache['total'] = copy.deepcopy(template)
    #         total_dis = BasicStatistic()
    #         for dis_name, dis_data in city_dis.items():
    #             if dis_name not in city_cache:
    #                 city_cache[dis_name] = copy.deepcopy(template)
    #                 continue
    #             cls._dict_append_data(city_cache, dis_name, dis_data, date)
    #             total_dis += dis_data
    #         cls._dict_append_data(city_cache, 'total', total_dis, date)
    #         cls.daily_cache_data[city] = city_cache



