#!/usr/bin/env python
# encoding=utf8

import ast

# ast.literal_eval()

from datetime import datetime, timedelta
from store import FileService, OSSClient
from config import ConfigService
from cache import CacheService, BasicStatistic
import logging
import os
import pickle
import shutil
import ast
import re
import util
import copy
import time
import constants
import pack_data


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
        self.listed_time = ""
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
               "看房量,状态,上架时间,成交周期,涨幅"

    def __repr__(self):
        return f"{self.ind},{self.total_price},{self.area}," \
               f"{format(self.unit_price, '5.2f')},{self.cmt_id},{self.cmt_name}," \
               f"{self.district},{self.sub_district}," \
               f"{self.city},{self.source},{self.fav_count}," \
               f"{self.view_count},{self.status},{self.listed_time},{self.deal_period},{self.vary}"


class ReportService:
    logger = logging.getLogger(__name__)

    num_re = r"\d*.\d+|\d+"

    file_time = ''
    tmp_dir = 'report_tmp_dir'

    total_data_files = 0

    daily_houses_string = House.get_header() + '\n'

    @classmethod
    def _reset(cls):
        cls.total_data_files = 0
        cls.daily_houses_string = House.get_header() + '\n'

    @classmethod
    def _load_data_file(cls, date):
        cls.logger.info(f"Load seeds hash and get the data file list.")
        data_file = constants.report_data_dir + '/' + date + constants.data_file_suffix
        if not os.path.exists(data_file):
            cls.logger.error(f"Data file {data_file} not exists! Can't generate report.")
            return
        FileService.unpack_file(data_file, cls.tmp_dir)
        # load seed file and new seed file
        # Just in order to be compatible with the old crawler package
        seed_file_old_version = cls.tmp_dir + '/' + date + '/content_page_seed_list'
        cls.seed_file = cls.tmp_dir + '/' + date + "/" + os.path.basename(constants.seeds_file) + '_' + date
        if os.path.exists(seed_file_old_version):
            if os.path.exists(cls.seed_file):
                os.remove(cls.seed_file)
            shutil.copy(seed_file_old_version, cls.seed_file)

        files = os.listdir(cls.tmp_dir + '/' + date + '/' + date)
        res = [cls.tmp_dir + '/' + date + '/' + date + '/' + file for file in files]
        return res

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
        house.listed_time = data["listed_time"]
        house.hash_code = data['hash_code']
        house.source = data['source']
        house.city = data['city']
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
        cls.total_data_files += 1

        house_cache = CacheService.get_house(house.hash_code)

        if "下架" in house.status or "成交" in house.status:
            # because the down may be crawled multi days, so remove it.
            if "下架" in house_cache['status'] or "成交" in house_cache['status']:
                # duplicate crawling
                return
            else:
                if not house.deal_period:
                    up_time = datetime.strptime(house.up_time, '%Y-%m-%d')
                    file_time = datetime.strptime(cls.file_time, '%Y-%m-%d')
                    house.deal_period = str((file_time - up_time).days)
                if "成交" in house.status:
                    target_date = house.listed_time
                else:
                    target_date = cls.file_time

                CacheService.update_daily_data('down', target_date, house.__dict__)
        else:
            CacheService.update_daily_data('total', cls.file_time, house.__dict__)

            if house_cache is None:
                CacheService.update_daily_data('up', house.listed_time, house.__dict__)

            price_now = house.total_price
            if house_cache:
                price_last = house_cache.total_price
                if price_last < price_now:
                    CacheService.update_daily_data('inc', cls.file_time, house.__dict__)
                    house.status = "涨价"
                elif price_last > price_now:
                    CacheService.update_daily_data('dec', cls.file_time, house.__dict__)
                    house.status = "降价"
                house.vary = price_now - price_last

        cls.daily_houses_string += str(house) + "\n"
        CacheService.update_house(house.__dict__)  # update the data

    @classmethod
    def _generate_daily_basic_report(cls):
        daily_data = CacheService.get_daily_data(cls.file_time)
        res = ''
        for city, dises in daily_data.items():
            res += f'{city}:\n'
            for dis_name, dis_data in dises.items():
                res += f'{dis_name}: {dis_data} \n'
        return res

    @classmethod
    def _send_notification(cls):
        cls.logger.info(f"Send notification.")
        daily_total_house_file = cls.file_time + "_house_status.csv"
        FileService.save_file(cls.tmp_dir, daily_total_house_file, cls.daily_houses_string, 'utf_8_sig')

        chart_address = "\n" + "曲线图：http://hkdev.yifei.me:8080/basic_statistic/" + "\n"

        basic_report = cls._generate_daily_basic_report()

        user_msg = f"{cls.file_time} \n" + basic_report + chart_address

        email_subject = f"{cls.file_time} 链家报告 test of new crawler"
        # util.send_mail("562315079@qq.com", "qlwhrvzayytcbche",
        #                #["562315079@qq.com", "kongyifei@gmail.com", "gaohangtian1003@163.com", "lbxxy@sina.com"],
        #                ["562315079@qq.com"],
        #                email_subject, user_msg, [cls.tmp_dir + "/" + daily_total_house_file])

        developer_msg = user_msg + '\n'

        print(email_subject + '\n')
        print(developer_msg)
        time.sleep(10)

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
    def gen_report(cls, file):
        if os.path.exists(cls.tmp_dir):
            shutil.rmtree(cls.tmp_dir)
        os.makedirs(cls.tmp_dir)

        cls._reset()

        filename = os.path.basename(file)
        end_index = filename.rindex(cls.data_file_suffix)
        date = filename[:end_index]

        cls.file_time = date
        cls.logger.info(f"the file time is {cls.file_time}")

        # check and unpack tar file
        data_files = cls._load_data_file(date)

        # process data files
        for file in data_files:
            cls._process_file(file)

        # send email
        cls._send_notification()

        CacheService.fresh_seed_file(cls.seed_file)

        # generate char data
        cls._gen_char_data()

        # upload today's data
        # cls.oss_client.upload_file(file)

    @classmethod
    def work(cls):
        cls.logger.info(f"Start generating report.")
        CacheService.start()

        pack_data.pack_report_data()

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



