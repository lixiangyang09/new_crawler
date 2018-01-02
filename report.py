#!/usr/bin/env python
# encoding=utf8

import ast

# ast.literal_eval()

from datetime import datetime, timedelta
from store import FileService, OSSClient
from config import ConfigService
from sync_set import SyncSet
import logging
import os
import pickle
import shutil
import ast
import re
import time
import util
import copy


class House:
    """hs for short"""
    def __init__(self):
        self.ind = ""
        self.unit_price = 0.0
        self.total_price = 0.0
        self.area = 0.0
        self.cmt_id = ""
        self.cmt_name = ""
        self.view_count = 0
        self.fav_count = 0
        self.up_time = ""
        self.hash_code = ""
        self.district = ""
        self.sub_district = ""
        self.city = ""
        self.source = ""
        self.status = ""
        self.deal_period = ""
        self.vary = 0

    @classmethod
    def get_header(cls):
        return "ID,总价,面积," \
               "单价,小区ID,小区名称," \
               "区县,区域," \
               "城市,来源,关注量," \
               "看房量,状态,成交周期,涨幅"

    def __repr__(self):
        return f"{self.ind},{self.total_price},{self.area}," \
               f"{format(self.unit_price, '5.2f')},{self.cmt_id},{self.cmt_name}," \
               f"{self.district},{self.sub_district}," \
               f"{self.city},{self.source},{self.fav_count}," \
               f"{self.view_count},{self.status},{self.deal_period},{self.vary}"


class BasicStatistic:
    def __init__(self):
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
            self.dec += ins.dev
        else:
            print(f"+= . Not an instance of BasicStatistic.")

    def __repr__(self):
        return f"在售, {self.total}, " \
               f"涨价, {self.inc}, " \
               f"降价, {self.dec}, " \
               f"上架, {self.up}, " \
               f"下架, {self.down}"


class Daily:
    def __init__(self):
        self.districts = dict()
        self.basic = BasicStatistic()
        self.file_count = 0
        self.seeds_count = 0
        self.ignore_count = 0
        self.daily_house_status = House.get_header() + "\n"
        self.ignore_data = ""
        self.seed_data_not_found_count = 0
        self.seed_data_not_found = ""

    def to_dict(self):
        res = dict()
        origin = self.__dict__
        for key, value in origin.items():
            if key == "districts":
                dis_res = dict()
                for dis_name, dis_inst in value.items():
                    dis_res[dis_name] = dis_inst.__dict__
                value = dis_res
            res[key] = value
        return res

    def generate_report(self):
        basic_report = ""
        for city, dis in self.districts.items():
            total_statis = BasicStatistic()
            for dis_name, dis_statis in dis.items():
                total_statis += dis_statis
            if city == 'bj':
                basic_report += f"{city},价格区间,200-1600,str{total_statis}"
            else:
                basic_report += f"{city},str{total_statis}"

            for dis_name, dis_statis in dis.items():
                basic_report += "/n"
                basic_report += f"{dis_name}, str{dis_statis}"
        return basic_report


class ReportService:
    logger = logging.getLogger(__name__)
    data_file_suffix = "_report_data.tar.gz"
    house_cache_file = 'house_cache'
    daily_cache_file = 'daily_cache'
    down_cache_file = 'down_cache'
    house_cache_data = dict()
    daily_cache_data = dict()
    down_cache_data = set()
    house_cur = dict()
    file_time = '2012-01-01'
    tmp_dir = 'report_tmp'
    base_dir = 'report_data'
    chart_data = 'chart_data'
    seeds = SyncSet()
    new_seeds = SyncSet()
    num_re = r"\d*.\d+|\d+"

    daily_data = Daily()

    seeds_to_delete = SyncSet()

    oss_client = OSSClient("buaacraft", "lxy", "/")

    city_map = {'beijing': 'bj'}

    @classmethod
    def _load_cache(cls, prefix):
        if prefix == 'house':
            file = cls.house_cache_file
        elif prefix == 'daily':
            file = cls.daily_cache_file
        elif prefix == 'down':
            file = cls.down_cache_file

        cls.logger.info(f"Try to load cache {file}.")
        if os.path.exists(file):
            with open(file, "rb") as f:
                cache_tmp = pickle.load(f)
            time = cache_tmp['time']
            data = cache_tmp['data']
            if prefix == 'house':
                cls.house_cache_data = data
            elif prefix == 'daily':
                cls.daily_cache_data = data
            elif prefix == 'down':
                cls.down_cache_data = data
            cls.logger.info(f"Finish load cache file {file} with {time}")
            return time
        else:
            cls.logger.warning(f"Can't find cache file {file}")
            return ""

    @classmethod
    def _load_data_file(cls, date):
        data_file = cls.base_dir + '/' + date + cls.data_file_suffix
        if not os.path.exists(data_file):
            cls.logger.error(f"Data file {data_file} not exists! Can't generate report.")
            return
        if os.path.exists(cls.tmp_dir):
            shutil.rmtree(cls.tmp_dir)
        os.mkdir(cls.tmp_dir)
        FileService.unpack_file(data_file, cls.tmp_dir)
        # load seed file and new seed file
        seed_file = cls.tmp_dir + '/' + date + "/" + os.path.basename(ConfigService.get_seeds_file())
        new_seed_file = cls.tmp_dir + '/' + date + "/" + os.path.basename(ConfigService.get_new_seeds_file())
        with open(seed_file) as f:
            for line in f:
                tokens = line.split(',')
                hash_code = tokens[0]
                if hash_code:
                    cls.seeds.add(hash_code)

        with open(new_seed_file) as f:
            for line in f:
                tokens = line.split(',')
                hash_code = tokens[0]
                if hash_code:
                    cls.new_seeds.add(hash_code)

        files = os.listdir(cls.tmp_dir + '/' + date + '/' + date)
        res = [cls.tmp_dir + '/' + date + '/' + date + '/' + file for file in files]
        return res

    @classmethod
    def _reset(cls):
        pass

    @classmethod
    def _get_city_map(cls, city):
        if city in cls.city_map:
            return cls.city_map[city]
        else:
            return city

    @classmethod
    def _parse_data_to_house(cls, file):
        print(file)

        data_str = FileService.load_file(file)
        data = ast.literal_eval(data_str)

        print(data_str)

        house = House()
        house.ind = data["id"]
        house.status = data['status']
        area_str = data["area"]
        house.area = float((re.findall(cls.num_re, area_str))[0])
        house.total_price = float(data["price"])
        house.unit_price = float(data["unit_price"]) / 10000.0
        house.up_time = data["listed_time"]
        house.hash_code = data['hash_code']
        house.source = data['source']
        house.city = cls._get_city_map(data['city'])
        house.fav_count = int(data["fav_count"])
        house.view_count = int(data["view_count"])

        house.deal_period = data.get('deal_period', "")

        cmt_link = data["community_link"]
        cmt_id = ""
        if cmt_link:
            cmt_id = cmt_link[cmt_link.rindex('/', 0, len(cmt_link) - 1) + 1:len(cmt_link) - 1]
        house.cmt_name = data["community_name"]
        house.district = data['district']
        house.sub_district = data['subdistrict']
        house.cmt_id = cmt_id
        house.cmt_link = cmt_link
        return house

    @classmethod
    def _process_file(cls, file):
        house = cls._parse_data_to_house(file)

        cls.daily_data.file_count += 1
        tmp_dis_city = cls.daily_data.districts.get(house.city, dict())
        tmp_dis = tmp_dis_city.get(house.district, BasicStatistic())

        if "下架" in house.status or "成交" in house.status:
            # because the down may be crawled multi days, so remove it.
            if house.hash_code not in cls.down_cache_data:
                tmp_dis.down += 1
                if not house.deal_period:
                    up_time = datetime.strptime(house.up_time, '%Y-%m-%d')
                    file_time = datetime.strptime(cls.file_time, '%Y-%m-%d')
                    house.deal_period = str((file_time - up_time).days)
                cls.daily_data.daily_house_status += str(house) + "\n"
            cls.down_cache_data.add(house.hash_code)
            cls.seeds_to_delete.add(house.hash_code)
            # remove the seed, because no need to crawl next day
        else:
            tmp_dis.total += 1

            if cls.new_seeds.exist(house.hash_code):
                house.status = "新上架"
                tmp_dis.up += 1

            price_now = house.total_price
            if house.hash_code in cls.house_cache_data:
                price_last = cls.house_cache_data[house.hash_code]['total_price']
                if price_last < price_now:
                    tmp_dis.inc += 1
                    house.status = "涨价"
                elif price_last > price_now:
                    tmp_dis.dec += 1
                    house.status = "降价"
                house.vary = price_now - price_last

            cls.daily_data.daily_house_status += str(house) + "\n"
        cls.daily_data.districts[house.city] = tmp_dis_city
        tmp_dis_city[house.district] = tmp_dis

        cls.house_cur[house.hash_code] = house.__dict__  # update the data

    @classmethod
    def _send_notification(cls):
        daily_total_house_file = "今日房源情况.csv"
        FileService.save_file(cls.tmp_dir, daily_total_house_file, cls.daily_data.daily_house_status, 'utf_8_sig')

        chart_address = "\n" + "曲线图：http://hkdev.yifei.me:8080/basic_statistic/" + "\n"
        basic_report = cls.daily_data.generate_report()
        # proxy_daily_report = proxy_report.gen_proxy_report()
        user_msg = f"{cls.file_time} \n" + basic_report + chart_address

        email_subject = f"{cls.file_time} 链家报告"
        util.send_mail("562315079@qq.com", "qlwhrvzayytcbche",
                       #["562315079@qq.com", "kongyifei@gmail.com", "gaohangtian1003@163.com", "lbxxy@sina.com"],
                       ["562315079@qq.com"],
                       email_subject, user_msg, [cls.tmp_dir + "/" + daily_total_house_file])

        time.sleep(60)

    @classmethod
    def _gen_char_data(cls):
        template = {'x_data': [], 'on': [], 'up': [], 'down': [], 'inc': [], 'dec': []}
        for city, data in cls.daily_data.districts.items():
            districts = {'total': copy.deepcopy(template)}
            total = BasicStatistic()
            for dis_name, dis_value in data.items():
                pass

    @classmethod
    def gen_report(cls, file):
        filename = os.path.basename(file)
        end_index = filename.rindex(cls.data_file_suffix)
        date = filename[:end_index]
        # reset parameters
        cls._reset()
        # check and unpack tar file
        data_files = cls._load_data_file(date)

        # load cache
        house_time = cls._load_cache('house')
        daily_time = cls._load_cache('daily')
        down_time = cls._load_cache('down')
        if house_time and daily_time and down_time and \
           house_time == daily_time and daily_time == down_time:
            cls.file_time = house_time
            cls.logger.info(f"Calculate statistics with cache of {cls.file_time}.")
        else:
            cls.logger.warning(f"Calculate statistics without cache.")
            cls.house_cache_data = dict()
            cls.daily_cache_data = dict()
            cls.down_cache_data = set()

        # process data files
        for file in data_files:
            cls._process_file(file)

        # send email
        cls._send_notification()
        # upload today's data
        cls.oss_client.upload_file(file)

        # generate char data
        cls._gen_char_data()

        # update the seeds file

    @classmethod
    def pack_today_data(cls):
        # 1. seed_list
        # 2. seed_list_new
        # 3. daily crawled data
        seed_file = ConfigService.get_seeds_file()
        if not os.path.exists(seed_file):
            cls.logger.warning(f"{seed_file} not exists")
            return

        new_seed_file = ConfigService.get_new_seeds_file()
        if not os.path.exists(new_seed_file):
            cls.logger.warning(f"{new_seed_file} not exists")
            return

        start_time = util.start_date
        tar_file = cls.base_dir + "/" + start_time + cls.data_file_suffix
        if not os.path.exists(tar_file):
            output_path = util.get_output_base_dir() + "/" + start_time
            if os.path.exists(output_path):
                # all there data are exist, copy them together
                target_dir = cls.base_dir + "/" + start_time
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                shutil.copyfile(seed_file, target_dir + "/" + os.path.basename(seed_file))
                shutil.copyfile(new_seed_file, target_dir + "/" + os.path.basename(new_seed_file))

                shutil.copytree(output_path, target_dir + "/" + start_time)
                # pack
                current_dir = os.getcwd()
                os.chdir(cls.base_dir)
                FileService.pack_folder(os.path.basename(tar_file), start_time, True)
                os.chdir(current_dir)
                cls.logger.info(f"packed data of {start_time} into {tar_file}")
            else:
                cls.logger.warning(f"path {output_path} not exists")
        else:
            cls.logger.info(f"{tar_file} already exist")

    @classmethod
    def get_process_file_list(cls):
        files = os.listdir(cls.base_dir)
        files.sort()
        res = []
        file_time = datetime.strptime(cls.file_time, '%Y-%m-%d')
        # 2017-10-09_report_data.tar.gz
        pattern = "(\d{4})-(\d{2})-(\d{2})" + cls.data_file_suffix + "$"
        data_file_pattern = re.compile(pattern)
        for tar_file in files:
            if data_file_pattern.match(tar_file):
                end_index = tar_file.rindex(cls.data_file_suffix)
                file_datetime_str = tar_file[:end_index]
                file_datetime = datetime.strptime(file_datetime_str, '%Y-%m-%d')
                if file_datetime > file_time:
                    res.append(cls.base_dir + "/" + tar_file)
        cls.process_file_list = res

    @classmethod
    def work(cls):
        cls.pack_today_data()
        cls.get_process_file_list()
        for file in cls.process_file_list:
            cls.gen_report(file)


if __name__ == '__main__':
    ReportService.work()

