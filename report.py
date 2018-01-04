#!/usr/bin/env python
# encoding=utf8

import ast

# ast.literal_eval()

from datetime import datetime, timedelta
from store import FileService, OSSClient
from config import ConfigService
from proxy import StatusService
from sync_set import SyncSet
import logging
import os
import pickle
import shutil
import ast
import re
import util
import copy
import time


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


class Daily:
    def __init__(self):
        #  districts[city] = dict()
        #  districts[city][dis_name] = BasicStatistic()
        self.districts = dict()
        self.file_count = 0

        self.daily_house_status = House.get_header() + "\n"

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
                basic_report += f"{city},价格区间,200-1600,{total_statis}"
            else:
                basic_report += f"{city},{total_statis}"

            for dis_name, dis_statis in dis.items():
                basic_report += "\n    "
                basic_report += f"{dis_name}, {dis_statis}"
            self.districts[city]['total'] = total_statis
        return basic_report


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
        now = datetime.now()
        for hash_code, date in self._data.items():
            hash_date = datetime.strptime(date, '%Y-%m-%d')
            if (now - hash_date) > timedelta(days=30):
                del self._data[hash_code]


class ReportService:
    logger = logging.getLogger(__name__)
    data_file_suffix = "_report_data.tar.gz"
    house_cache_file = 'house_cache'
    daily_cache_file = 'daily_cache'
    down_cache_file = 'down_cache'
    house_cache_data = dict()
    daily_cache_data = dict()
    down_cache_data = MarkDict()
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
    chart_data_path = "chart_data"

    @classmethod
    def _load_cache(cls, prefix):
        cls.logger.info(f"load cache of {prefix}")
        if prefix == 'house':
            file = cls.house_cache_file
        elif prefix == 'daily':
            file = cls.daily_cache_file
        elif prefix == 'down':
            file = cls.down_cache_file

        file = cls.base_dir + '/' + file

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
        cls.logger.info(f"Load seeds hash and get the data file list.")
        data_file = cls.base_dir + '/' + date + cls.data_file_suffix
        if not os.path.exists(data_file):
            cls.logger.error(f"Data file {data_file} not exists! Can't generate report.")
            return
        if os.path.exists(cls.tmp_dir):
            shutil.rmtree(cls.tmp_dir)
        os.mkdir(cls.tmp_dir)
        FileService.unpack_file(data_file, cls.tmp_dir)
        # load seed file and new seed file
        # Just in order to be compatible with the old crawler package
        seed_file_old_version = cls.tmp_dir + '/' + date + '/content_page_seed_list'
        seed_file = cls.tmp_dir + '/' + date + "/" + os.path.basename(ConfigService.get_seeds_file()) + '_' + date
        if os.path.exists(seed_file_old_version):
            if os.path.exists(seed_file):
                os.remove(seed_file)
            shutil.copy(seed_file_old_version, seed_file)

        new_seed_file_old_version = cls.tmp_dir + '/' + date + '/content_page_seed_list_new'
        new_seed_file = cls.tmp_dir + '/' + date + "/" + os.path.basename(ConfigService.get_new_seeds_file()) + '_' + date
        if os.path.exists(new_seed_file_old_version):
            if os.path.exists(new_seed_file):
                os.remove(new_seed_file)
            shutil.copy(new_seed_file_old_version, new_seed_file)

        with open(seed_file) as f:
            for line in f:
                line_tmp = line.strip()
                if line_tmp:
                    tokens = line.split(',')
                    hash_code = tokens[0]
                    cls.seeds.add(hash_code)

        with open(new_seed_file) as f:
            for line in f:
                line_tmp = line.strip()
                if line_tmp:
                    tokens = line.split(',')
                    hash_code = tokens[0]
                    cls.new_seeds.add(hash_code)

        files = os.listdir(cls.tmp_dir + '/' + date + '/' + date)
        res = [cls.tmp_dir + '/' + date + '/' + date + '/' + file for file in files]
        return res

    @classmethod
    def _reset(cls):
        cls.logger.info(f"Reset parameters.")
        cls.seeds = SyncSet()
        cls.new_seeds = SyncSet()
        cls.daily_data = Daily()
        cls.seeds_to_delete = SyncSet()

    @classmethod
    def _get_city_map(cls, city):
        if city in cls.city_map:
            return cls.city_map[city]
        else:
            return city

    @classmethod
    def _parse_data_to_house(cls, file):
        data_str = FileService.load_file(file)
        data = ast.literal_eval(data_str)

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
        cls.logger.info(f"Processing {file} with data hash {house.hash_code}")
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
            cls.down_cache_data.add(house.hash_code, cls.file_time)
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
        cls.logger.info(f"Send notification.")
        daily_total_house_file = cls.file_time + "_house_status.csv"
        FileService.save_file(cls.tmp_dir, daily_total_house_file, cls.daily_data.daily_house_status, 'utf_8_sig')

        chart_address = "\n" + "曲线图：http://hkdev.yifei.me:8080/basic_statistic/" + "\n"
        basic_report = cls.daily_data.generate_report()
        proxy_daily_report = StatusService.report()

        user_msg = f"{cls.file_time} \n" + basic_report + chart_address

        email_subject = f"{cls.file_time} 链家报告 test of new crawler"
        # util.send_mail("562315079@qq.com", "qlwhrvzayytcbche",
        #                #["562315079@qq.com", "kongyifei@gmail.com", "gaohangtian1003@163.com", "lbxxy@sina.com"],
        #                ["562315079@qq.com"],
        #                email_subject, user_msg, [cls.tmp_dir + "/" + daily_total_house_file])

        developer_msg = user_msg + '\n' + proxy_daily_report

        print(email_subject + '\n')
        print(developer_msg)
        time.sleep(3)

    @classmethod
    def _gen_char_data(cls):
        wanted_order = ['total', '海淀', '西城', '东城', '房山', '']
        template = {'x_data': [], 'on': [], 'up': [], 'down': [], 'inc': [], 'dec': []}
        result_districts = dict()
        for file_date, data in cls.daily_cache_data.items():
            for city, districts in data.items():
                if city not in result_districts:
                    result_districts[city] = dict()
                city_districts = result_districts[city]
                for dis_name, dis_value in districts.items():
                    if dis_name not in city_districts:
                        city_districts[dis_name] = copy.deepcopy(template)
                        continue  # ignore the data of first day, because all the data is marked as up.
                    city_districts[dis_name]['x_data'].append(file_date)
                    city_districts[dis_name]['on'].append(dis_value.total)
                    city_districts[dis_name]['up'].append(dis_value.up)
                    city_districts[dis_name]['down'].append(dis_value.down)
                    city_districts[dis_name]['inc'].append(dis_value.inc)
                    city_districts[dis_name]['dec'].append(dis_value.dec)

        # Reorder
        for city, data in result_districts.items():
            ordered_data = []
            for dis_name in wanted_order:
                if dis_name in data:
                    display_name = dis_name
                    if display_name == '':
                        display_name = 'abnormal'
                    ordered_data.append((display_name, data[dis_name]))
            if not os.path.exists(cls.chart_data_path):
                os.makedirs(cls.chart_data_path)
            chart_data_file = cls.chart_data_path + '/' + city
            with open(chart_data_file, "wb") as f:
                pickle.dump(ordered_data, f)
            shutil.copy(chart_data_file, chart_data_file + '_' + cls.file_time)

    @classmethod
    def _update_seeds(cls):
        """
        update seeds by removing the off shelf seeds for next time use
        :return:
        """
        old_seeds_file = cls.tmp_dir + '/' + cls.file_time + "/" + os.path.basename(ConfigService.get_seeds_file())
        new_seeds_file = ConfigService.get_seeds_file()

        cls.logger.info(f"Update seeds from {old_seeds_file} to {new_seeds_file}")

        with open(old_seeds_file) as in_file:
            with open(new_seeds_file, 'w') as out_file:
                for line in in_file:
                    tmp_line = line.strip()
                    if tmp_line:
                        hash_code = tmp_line.split(',')[0]
                        if not cls.seeds_to_delete.exist(hash_code):
                            out_file.write(line)
        if os.path.exists(ConfigService.get_new_seeds_file()):
            os.remove(ConfigService.get_new_seeds_file())

    @classmethod
    def _save_cache(cls):
        cls.logger.info(f"Save cache.")
        cls.daily_cache_data[cls.file_time] = cls.daily_data.districts
        with open(cls.base_dir + '/' + cls.house_cache_file, "wb") as f:
            pickle.dump(dict(time=cls.file_time, data=cls.house_cur), f)
        with open(cls.base_dir + '/' + cls.daily_cache_file, "wb") as f:
            pickle.dump(dict(time=cls.file_time, data=cls.daily_cache_data), f)
        cls.down_cache_data.house_keep()
        with open(cls.base_dir + '/' + cls.down_cache_file, "wb") as f:
            pickle.dump(dict(time=cls.file_time, data=cls.down_cache_data), f)
        shutil.copy(cls.base_dir + '/' + cls.house_cache_file,
                    cls.base_dir + '/' + cls.house_cache_file + '_' + cls.file_time)
        shutil.copy(cls.base_dir + '/' + cls.daily_cache_file,
                    cls.base_dir + '/' + cls.daily_cache_file + '_' + cls.file_time)
        shutil.copy(cls.base_dir + '/' + cls.down_cache_file,
                    cls.base_dir + '/' + cls.down_cache_file + '_' + cls.file_time)
        cls.logger.info("Saving cache files.")

    @classmethod
    def pack_data(cls, data_folder):
        # 1. seed_list
        # 2. seed_list_new
        # 3. daily crawled data
        cls.logger.info(f"Ready to pack the data of {util.start_date}")
        seed_file = ConfigService.get_seeds_file() + '_' + data_folder
        if not os.path.exists(seed_file):
            cls.logger.warning(f"{seed_file} not exists, continue without pack today's data.")
            return

        new_seed_file = ConfigService.get_new_seeds_file() + '_' + data_folder
        if not os.path.exists(new_seed_file):
            cls.logger.warning(f"{new_seed_file} not exists, continue without pack today's data.")
            return

        tar_file = cls.base_dir + "/" + data_folder + cls.data_file_suffix
        if not os.path.exists(tar_file):
            output_path = util.get_output_base_dir() + "/" + data_folder
            if os.path.exists(output_path):
                # all there data are exist, copy them together
                target_dir = cls.base_dir + "/" + data_folder
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                shutil.copyfile(seed_file, target_dir + "/" + os.path.basename(seed_file) + '_' + data_folder)
                shutil.copyfile(new_seed_file, target_dir + "/" + os.path.basename(new_seed_file) + '_' + data_folder)

                shutil.copytree(output_path, target_dir + "/" + data_folder)
                # pack
                current_dir = os.getcwd()
                os.chdir(cls.base_dir)
                FileService.pack_folder(os.path.basename(tar_file), data_folder, True)
                os.chdir(current_dir)
                cls.logger.info(f"packed data of {start_time} into {tar_file}")
            else:
                cls.logger.warning(f"data path {output_path} not exists, continue without pack today's data.")
        else:
            cls.logger.info(f"{tar_file} already exist")

    @classmethod
    def get_process_file_list(cls):
        files = os.listdir(cls.base_dir)
        files.sort()
        file_time = datetime.strptime(cls.file_time, '%Y-%m-%d')
        # 2017-10-09_report_data.tar.gz
        pattern = "(\d{4})-(\d{2})-(\d{2})" + cls.data_file_suffix + "$"
        data_file_pattern = re.compile(pattern)
        res = []
        for tar_file in files:
            if data_file_pattern.match(tar_file):
                end_index = tar_file.rindex(cls.data_file_suffix)
                file_datetime_str = tar_file[:end_index]
                file_datetime = datetime.strptime(file_datetime_str, '%Y-%m-%d')
                if file_datetime > file_time:
                    res.append(cls.base_dir + "/" + tar_file)
        cls.logger.info(f"The process file list is: {res}")
        return res

    @classmethod
    def get_cache_file_time(cls):
        # load cache
        house_time = cls._load_cache('house')
        daily_time = cls._load_cache('daily')
        down_time = cls._load_cache('down')
        if house_time and daily_time and down_time and \
           house_time == daily_time and daily_time == down_time:
            cls.file_time = house_time
            cls.logger.info(f"Calculate statistics with cache of {cls.file_time}.")
            cls.house_cache_data = dict()
            cls.daily_cache_data = dict()
            cls.down_cache_data = MarkDict()
        else:
            cls.logger.warning(f"Calculate statistics without cache.")

    @classmethod
    def get_pack_list(cls):
        pattern = "^(\d{4})-(\d{2})-(\d{2})$"
        data_folder_pattern = re.compile(pattern)
        file_time = datetime.strptime(cls.file_time, '%Y-%m-%d')
        output_list = os.listdir(util.get_output_base_dir())
        res = []
        for file in output_list:
            file = util.get_output_base_dir() + '/' + file
            file_name = os.path.basename(file)
            if os.path.isdir(file) \
                    and data_folder_pattern.match(file_name):
                file_datetime = datetime.strptime(file_name, '%Y-%m-%d')
                if file_datetime > file_time:
                    res.append(file_name)

        return res

    @classmethod
    def gen_report(cls, file):
        filename = os.path.basename(file)
        end_index = filename.rindex(cls.data_file_suffix)
        date = filename[:end_index]

        cls.file_time = date
        cls.logger.info(f"the file time is {cls.file_time}")
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
            cls.logger.info(f"Calculate statistics with cache of {cls.file_time}.")
        else:
            cls.logger.warning(f"Calculate statistics without cache.")
            cls.house_cache_data = dict()
            cls.daily_cache_data = dict()
            cls.down_cache_data = MarkDict()

        # process data files
        for file in data_files:
            cls._process_file(file)

        # send email
        cls._send_notification()

        # save cache
        cls._save_cache()
        # update seeds
        cls._update_seeds()

        # upload today's data
        # cls.oss_client.upload_file(file)

        # generate char data
        cls._gen_char_data()

    @classmethod
    def work(cls):
        cls.logger.info(f"Start generating report.")
        cls.get_cache_file_time()
        pack_list = cls.get_pack_list()
        cls.logger.info(f"Pack list: {pack_list}")
        for pack in pack_list:
            cls.pack_data(pack)
        process_files = cls.get_process_file_list()
        if not process_files:
            cls.logger.warning(f"No need to generate report, because of the data is not newer than cache.")
        for file in process_files:
            cls.logger.info(f"Begin to process file {file}")
            cls.gen_report(file)


if __name__ == '__main__':
    ReportService.work()


