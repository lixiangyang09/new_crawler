#!/usr/bin/env python
# encoding=utf8


# Three cache file for
# 1. month存量房网上签约总量
# 2. month存量房网上签约总量，各区县
# 3. day存量房网上签约
# 4. day核验房源

import constants
import pickle
import os
import util
import re
import ast
import store


class BJJianweiReport():

    monthly_detail_data = dict()
    monthly_overview_data = dict()
    daily_check_data = dict()
    daily_signed_data = dict()

    cache_field = 'cache'
    last_time = 'last'
    title_field = 'title'
    date_field = 'date'

    @classmethod
    def load_cache(cls, data_file):
        if os.path.exists(data_file):
            with open(data_file, 'rb') as input_file:
                data = pickle.load(input_file)
        else:
            data = dict()
        return data

    @classmethod
    def _get_parse_title(cls, data):
        pattern = re.compile("(\d{4}-\d{2}-\d{2})(.*)")
        match_res = pattern.match(data)
        if match_res:
            return match_res.group(1), match_res.group(2)
        else:
            ind = data.rfind('月')
            return data[:ind + 1], data[ind + 1:]

    @classmethod
    def _update_field(cls, data, key, value):
        tmp = data.get(key, list())
        tmp.append(value)
        data[key] = tmp

    @classmethod
    def update_monthly_detail(cls, data):
        """
        {last: title of last time,
        date: list of time,
        data: {
            name: {
            count: list,
            area: list}
            }
        }
        """
        last_time_title = cls.monthly_detail_data.get(cls.last_time, '')
        if data[cls.title_field] > last_time_title:
            cls.monthly_detail_data[cls.last_time] = data[cls.title_field]
            date, name = cls._get_parse_title(data[cls.title_field])
            cls._update_field(cls.monthly_detail_data, cls.title_field, data[cls.title_field])
            cls._update_field(cls.monthly_detail_data, cls.date_field, date)
            cache_data = cls.monthly_detail_data.get('data', dict())
            region = data['区县']
            count = data['套数']
            area = data['成交面积']
            for index in range(0, len(region)):
                region_name = region[index] + name
                region_data = cache_data.get(region_name, dict())
                cls._update_field(region_data, 'count', int(count[index]))
                cls._update_field(region_data, 'area', float(area[index]) / 100)
                region_data['date'] = cls.monthly_detail_data[cls.date_field]
                region_data['name'] = region_name
                cache_data[region_name] = region_data
            cls.monthly_detail_data['data'] = cache_data

    @classmethod
    def update_chart(cls, data, target):
        """
        {title:,
        date:,
        field_name:,}
        """
        last_time_title = target.get(cls.last_time, '')
        if data[cls.title_field] > last_time_title:
            target[cls.last_time] = data[cls.title_field]
            date, name = cls._get_parse_title(data[cls.title_field])
            target[cls.title_field] = name
            cls._update_field(target, 'date', date)
            for key, value in data.items():
                if key == 'title':
                    continue
                if '面积' in key:
                    store_value = float(value)/100
                else:
                    store_value = float(value)
                cls._update_field(target, key, store_value)

    @classmethod
    def process_data(cls):
        data_file_list = sorted(os.listdir(util.get_jianwei_data_dir()))
        # 2017-10-09
        file_pattern = re.compile("(\d{4})-(\d{2})-(\d{2})")
        for file in data_file_list:
            print(file)
            if file_pattern.match(file):
                data = store.FileService.load_file(os.path.join(util.get_jianwei_data_dir(), file))
                data_list = ast.literal_eval(data)

                cls.update_monthly_detail(data_list[0])
                cls.update_chart(data_list[1], cls.daily_check_data)
                cls.update_chart(data_list[2], cls.monthly_overview_data)
                cls.update_chart(data_list[3], cls.daily_signed_data)

    @classmethod
    def save_chart_data(cls):
        with open(constants.monthly_overview_file, 'wb') as output_file:
            pickle.dump(cls.monthly_overview_data, output_file)
        with open(constants.monthly_detail_file, 'wb') as output_file:
            pickle.dump(cls.monthly_detail_data, output_file)
        with open(constants.daily_signed_file, 'wb') as output_file:
            pickle.dump(cls.daily_signed_data, output_file)
        with open(constants.daily_check_file, 'wb') as output_file:
            pickle.dump(cls.daily_check_data, output_file)

    @classmethod
    def work(cls):
        cls.monthly_detail_data = cls.load_cache(constants.monthly_detail_file)
        cls.monthly_overview_data = cls.load_cache(constants.monthly_overview_file)
        cls.daily_check_data = cls.load_cache(constants.daily_check_file)
        cls.daily_signed_data = cls.load_cache(constants.daily_signed_file)

        cls.process_data()
        cls.save_chart_data()

if __name__ == '__main__':
    BJJianweiReport.work()

