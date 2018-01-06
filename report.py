#!/usr/bin/env python
# encoding=utf8

import ast

# ast.literal_eval()

from datetime import datetime, timedelta
from store import FileService, OSSClient
from config import ConfigService
from cache import CacheService
import logging
import os
import pickle
import shutil
import ast
import re
import util
import copy
import time

if not os.path.exists('log'):
    os.mkdir('log')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)15s[%(lineno)4d] %(levelname)8s %(thread)d %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    handlers=[logging.FileHandler(
                        filename="./log/" + util.start_date + "_report.log", # str(datetime.now()).replace(" ", "_").replace(":", "_") + "_crawler.log",
                        mode='w', encoding="utf-8")])


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


class Daily:
    def __init__(self):
        #  districts[city] = dict()
        #  districts[city][dis_name] = BasicStatistic()
        self.districts = dict()
        self.file_count = 0

        self.daily_house_status = House.get_header() + "\n"

    def generate_report(self):
        basic_report = ""
        for city, dis in self.districts.items():
            total_statis = BasicStatistic()
            if city == 'bj':
                basic_report += f"{city},价格区间,200-1600 \n"
            else:
                basic_report += f"{city} \n"
            for dis_name, dis_statis in dis.items():
                basic_report += "\n    "
                basic_report += f"{dis_name}, {dis_statis}"
                total_statis += dis_statis
            self.districts[city]['total'] = total_statis
        return basic_report

 
class ReportService:
    logger = logging.getLogger(__name__)
    data_file_suffix = "_report_data.tar.gz"

    tmp_dir = 'report_tmp'
    base_dir = 'report_data'

    num_re = r"\d*.\d+|\d+"

    daily_data = Daily()

    seeds_to_delete = set()

    oss_client = OSSClient("buaacraft", "lxy", "/")

    city_map = {'beijing': 'bj'}
    chart_data_path = "chart_data"

    new_seeds = set()

    file_time = ''

    @classmethod
    def _load_data_file(cls, date):
        cls.logger.info(f"Load seeds hash and get the data file list.")
        data_file = cls.base_dir + '/' + date + cls.data_file_suffix
        if not os.path.exists(data_file):
            cls.logger.error(f"Data file {data_file} not exists! Can't generate report.")
            return
        if os.path.exists(cls.tmp_dir + '/' + date):
            shutil.rmtree(cls.tmp_dir + '/' + date)

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
        new_seed_file = cls.tmp_dir + '/' + date + "/" + \
            os.path.basename(ConfigService.get_new_seeds_file()) + '_' + date

        if os.path.exists(new_seed_file_old_version):
            if os.path.exists(new_seed_file):
                os.remove(new_seed_file)
            shutil.copy(new_seed_file_old_version, new_seed_file)

        cls.new_seeds = CacheService.generate_new_seed_cache(new_seed_file)

        CacheService.load_seed_file(seed_file)

        files = os.listdir(cls.tmp_dir + '/' + date + '/' + date)
        res = [cls.tmp_dir + '/' + date + '/' + date + '/' + file for file in files]
        return res

    @classmethod
    def _reset(cls):
        cls.logger.info(f"Reset parameters.")
        cls.daily_data = Daily()

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
        if not house.district:
            cls.logger.error(f"Abnormal data: {house}")

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
            if not CacheService.already_off_shelf(house.hash_code, cls.file_time):
                tmp_dis.down += 1
                if not house.deal_period:
                    up_time = datetime.strptime(house.up_time, '%Y-%m-%d')
                    file_time = datetime.strptime(cls.file_time, '%Y-%m-%d')
                    house.deal_period = str((file_time - up_time).days)
            else:
                house.status = 'duplicate crawled'
            # remove the seed, because no need to crawl next day
        else:
            tmp_dis.total += 1

            if house.hash_code in cls.new_seeds:
                house.status = "新上架"
                tmp_dis.up += 1

            price_now = house.total_price
            last_house_data = CacheService.get_house(house.hash_code)
            if last_house_data:
                price_last = last_house_data['total_price']
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
        CacheService.update_house(house.__dict__)  # update the data

    @classmethod
    def _send_notification(cls):
        cls.logger.info(f"Send notification.")
        daily_total_house_file = cls.file_time + "_house_status.csv"
        FileService.save_file(cls.tmp_dir, daily_total_house_file, cls.daily_data.daily_house_status, 'utf_8_sig')

        chart_address = "\n" + "曲线图：http://hkdev.yifei.me:8080/basic_statistic/" + "\n"
        basic_report = cls.daily_data.generate_report()

        user_msg = f"{cls.file_time} \n" + basic_report + chart_address

        email_subject = f"{cls.file_time} 链家报告 test of new crawler"
        # util.send_mail("562315079@qq.com", "qlwhrvzayytcbche",
        #                #["562315079@qq.com", "kongyifei@gmail.com", "gaohangtian1003@163.com", "lbxxy@sina.com"],
        #                ["562315079@qq.com"],
        #                email_subject, user_msg, [cls.tmp_dir + "/" + daily_total_house_file])

        developer_msg = user_msg + '\n'

        print(email_subject + '\n')
        print(developer_msg)
        time.sleep(3)

    @classmethod
    def _dict_append_data(cls, base, name, input_data, date):
        base[name]['x_data'].append(date)
        base[name]['on'].append(input_data.total)
        base[name]['up'].append(input_data.up)
        base[name]['down'].append(input_data.down)
        base[name]['inc'].append(input_data.inc)
        base[name]['dec'].append(input_data.dec)

    @classmethod
    def _gen_char_data(cls):
        wanted_order = ['total', '海淀', '西城', '东城', '房山', '昌平',
                        '高新西', '天府新区', '武侯', '成华', '金牛', '青羊', '锦江', '']
        # Reorder
        for city, data in CacheService.daily_cache_data.items():
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
    def pack_data(cls, data_folder):
        # 1. seed_list
        # 2. seed_list_new
        # 3. daily crawled data
        cls.logger.info(f"Ready to pack the data folder {data_folder}")
        seed_file = ConfigService.get_seeds_file() + '_' + data_folder
        if not os.path.exists(seed_file):
            cls.logger.warning(f"{seed_file} not exists, continue without pack today's data.")
            return

        new_seed_file = ConfigService.get_new_seeds_file() + '_' + data_folder
        if not os.path.exists(new_seed_file):
            cls.logger.warning(f"{new_seed_file} not exists, continue without pack today's data.")
            return

        tar_file = cls.base_dir + "/" + data_folder + cls.data_file_suffix
        if os.path.exists(tar_file):
            cls.logger.warning(f"File {tar_file} already exist, no need to pack again.")
            return
        if not os.path.exists(tar_file):
            output_path = util.get_output_base_dir() + "/" + data_folder
            if os.path.exists(output_path):
                # all there data are exist, copy them together
                target_dir = cls.base_dir + "/" + data_folder
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                shutil.copyfile(seed_file, target_dir + "/" + os.path.basename(seed_file))
                shutil.copyfile(new_seed_file, target_dir + "/" + os.path.basename(new_seed_file))

                shutil.copytree(output_path, target_dir + "/" + data_folder)
                # pack
                current_dir = os.getcwd()
                os.chdir(cls.base_dir)
                FileService.pack_folder(os.path.basename(tar_file), data_folder, True)
                os.chdir(current_dir)
                cls.logger.info(f"packed data folder of {data_folder} into {tar_file}")
            else:
                cls.logger.warning(f"data path {output_path} not exists, continue without pack today's data.")
        else:
            cls.logger.info(f"{tar_file} already exist")

    @classmethod
    def get_process_file_list(cls):
        files = os.listdir(cls.base_dir)
        files.sort()
        file_time = datetime.strptime(CacheService.cache_time, '%Y-%m-%d')
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
    def get_pack_list(cls):
        pattern = "^(\d{4})-(\d{2})-(\d{2})$"
        data_folder_pattern = re.compile(pattern)
        file_time = datetime.strptime(CacheService.cache_time, '%Y-%m-%d')
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

        # process data files
        for file in data_files:
            cls._process_file(file)

        # send email
        cls._send_notification()

        CacheService.update_daily_cache(cls.file_time, cls.daily_data.districts)

        # generate char data
        cls._gen_char_data()

        # upload today's data
        # cls.oss_client.upload_file(file)

    @classmethod
    def work(cls):
        cls.logger.info(f"Start generating report.")
        if os.path.exists(cls.tmp_dir):
            shutil.rmtree(cls.tmp_dir)
        os.mkdir(cls.tmp_dir)
        CacheService.start()
        pack_list = cls.get_pack_list()
        cls.logger.info(f"Pack list: {pack_list}")
        for pack in pack_list:
            cls.pack_data(pack)
        process_files = cls.get_process_file_list()
        if process_files:
            for file in process_files:
                cls.logger.info(f"Begin to process file {file}")
                cls.gen_report(file)
            # now the cls.file_time saved the latest date of data
            CacheService.stop(cls.file_time)
        else:
            cls.logger.warning(f"No need to generate report, because of the data is not newer than cache.")


if __name__ == '__main__':
    ReportService.work()



