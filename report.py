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
               f"{format(self.unit_price, '.2f')},{self.cmt_id},{self.cmt_name}," \
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
    oss_client = OSSClient("buaacraft", "lxy_new_crawl", "/")

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
        cls.seed_file = cls.tmp_dir + "/" + os.path.basename(constants.seeds_file) + '_' + date
        if os.path.exists(seed_file_old_version):
            if os.path.exists(cls.seed_file):
                os.remove(cls.seed_file)
            shutil.copy(seed_file_old_version, cls.seed_file)

        files = os.listdir(cls.tmp_dir + '/' + date)
        res = [cls.tmp_dir + '/' + date + '/' + file for file in files if not file.startswith('.')]
        return res

    @classmethod
    def _parse_data_to_house(cls, file):
        data_str = FileService.load_file(file)
        data = ast.literal_eval(data_str)

        check_list = ['district', 'listed_time']
        for check in check_list:
            if check not in data:
                cls.logger.error(f"Abnormal data! field {check} not in {data} of file {file}")
                return None

        house = House()
        house.ind = data["id"]
        house.status = data['status']
        area_str = data["area"]
        house.area = float((re.findall(cls.num_re, area_str))[0])
        house.total_price = float(data["price"])
        house.unit_price = float(data["unit_price"]) / 10000.0
        house.listed_time = data["listed_time"]
        if not house.listed_time:
            house.listed_time = cls.file_time
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
        return house

    @classmethod
    def _process_file(cls, file):
        house = cls._parse_data_to_house(file)
        if house is None:
            return
        cls.total_data_files += 1

        house_cache = CacheService.get_house(house.hash_code)

        if "下架" in house.status or "成交" in house.status:
            # because the down may be crawled multi days, so remove it.
            if house_cache and ("下架" in house_cache['status'] or "成交" in house_cache['status']):
                # duplicate crawling
                return
            else:
                if not house.deal_period:
                    up_time = datetime.strptime(house.listed_time, '%Y-%m-%d')
                    file_time = datetime.strptime(cls.file_time, '%Y-%m-%d')
                    house.deal_period = str((file_time - up_time).days)
                if "成交" in house.status:
                    target_date = house.status.strip().split(' ')[0].replace('.', '-')
                else:
                    target_date = cls.file_time

                CacheService.update_daily_data('down', target_date, house.__dict__)
        else:
            CacheService.update_daily_data('total', cls.file_time, house.__dict__)

            if house_cache is None:
                CacheService.update_daily_data('up', house.listed_time, house.__dict__)
                CacheService.update_daily_data('total', house.listed_time, house.__dict__)

            if house_cache and "下架" in house_cache['status']:
                # "下架" in house_cache.status, the house is back to sell again.
                CacheService.update_daily_data('up', cls.file_time, house.__dict__)

            price_now = house.total_price
            if house_cache:
                price_last = house_cache['total_price']
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
        res = f'Total data file: {cls.total_data_files}\n'
        for city, dises in daily_data.items():
            res += f'{city}:\n'
            for dis_name, dis_data in dises.items():
                res += f'    {dis_name}: '
                for dis_field_name, dis_field_value in dis_data.items():
                    res += f'{dis_field_name}: {dis_field_value}, '
                res += '\n'
        return res

    @classmethod
    def _send_notification(cls):
        cls.logger.info(f"Send notification.")
        daily_total_house_file = cls.file_time + "_house_status.csv"
        FileService.save_file(constants.notifies_dir, daily_total_house_file, cls.daily_houses_string, 'utf_8_sig')

        chart_address = "\n" + "北京：http://stats.yifei.me/basic_statistic/bj/" + "\n" \
                             + "成都：http://stats.yifei.me/basic_statistic/cd/" + "\n"
        basic_report = cls._generate_daily_basic_report()

        note_msg = "若当天的下架数量,涨价数量，降价数量同时为0时，有可能是当天数据爬取失败。\n"

        user_msg = f"{cls.file_time} \n" + basic_report + chart_address + note_msg

        email_subject = f"{cls.file_time} 链家报告"
        util.send_mail("562315079@qq.com", "qlwhrvzayytcbche",
                       ["562315079@qq.com", "kongyifei@gmail.com", "gaohangtian1003@163.com", "lbxxy@sina.com"],
                       # ["562315079@qq.com"],
                       email_subject, user_msg, [constants.notifies_dir + "/" + daily_total_house_file])

        developer_msg = user_msg + '\n'

        cls.logger.info(developer_msg)
        print(email_subject + '\n')
        print(developer_msg)
        # time.sleep(10)

    @classmethod
    def _dict_append_data(cls, base, name, input_data, date):
        base[name]['x_data'].append(date)
        base[name]['on'].append(input_data['total'])
        base[name]['up'].append(input_data['up'])
        base[name]['down'].append(input_data['down'])
        base[name]['inc'].append(input_data['inc'])
        base[name]['dec'].append(input_data['dec'])

    @classmethod
    def _gen_char_data(cls):
        daily_data = CacheService.get_daily_data()
        template = {'x_data': [], 'on': [], 'up': [], 'down': [], 'inc': [], 'dec': []}
        result = dict()

        for file_date, city_data in daily_data.items():
            for city, dis_data in city_data.items():
                if city not in result:
                    result[city] = dict()
                res_dis = result[city]
                for dis_name, dis_value in dis_data.items():
                    if dis_name not in res_dis:
                        res_dis[dis_name] = copy.deepcopy(template)
                    cls._dict_append_data(res_dis, dis_name, dis_value, file_date)

        wanted_order = ['total', '海淀', '西城', '东城', '房山', '昌平',
                        '高新西', '天府新区', '武侯', '成华', '金牛', '青羊', '锦江', '']
        # Reorder
        for city, data in result.items():
            ordered_data = []
            for dis_name in wanted_order:
                if dis_name in data:
                    display_name = dis_name
                    if display_name == '':
                        display_name = 'abnormal'
                    ordered_data.append((display_name, data[dis_name]))
            if not os.path.exists(constants.chart_data_dir):
                os.makedirs(constants.chart_data_dir)
            chart_data_file = constants.chart_data_dir + '/' + city
            with open(chart_data_file, "wb") as f:
                pickle.dump(ordered_data, f)
            shutil.copy(chart_data_file, chart_data_file + '_' + cls.file_time)

    @classmethod
    def get_process_file_list(cls):
        files = os.listdir(constants.report_data_dir)
        files.sort()
        file_time = datetime.strptime(CacheService.cache_date, '%Y-%m-%d')
        # 2017-10-09_report_data.tar.gz
        pattern = "(\d{4})-(\d{2})-(\d{2})" + constants.data_file_suffix + "$"
        data_file_pattern = re.compile(pattern)
        res = []
        for tar_file in files:
            if data_file_pattern.match(tar_file):
                end_index = tar_file.rindex(constants.data_file_suffix)
                file_datetime_str = tar_file[:end_index]
                file_datetime = datetime.strptime(file_datetime_str, '%Y-%m-%d')
                if file_datetime > file_time:
                    res.append(constants.report_data_dir + "/" + tar_file)
        cls.logger.info(f"The process file list is: {res}")
        return res

    @classmethod
    def gen_report(cls, packed_file):
        cls.logger.info(f"Generate report of {packed_file}")
        if os.path.exists(cls.tmp_dir):
            shutil.rmtree(cls.tmp_dir)
        os.makedirs(cls.tmp_dir)

        cls._reset()

        filename = os.path.basename(packed_file)
        end_index = filename.rindex(constants.data_file_suffix)
        date = filename[:end_index]

        CacheService.assume_start_date(date)

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

        # upload today's data
        cls.oss_client.upload_file(packed_file)

        if os.path.exists(cls.tmp_dir):
            shutil.rmtree(cls.tmp_dir)

    @classmethod
    def house_keeping(cls):
        """
        This function is used to clean the out of date data.
        output folder,
        database folder,
        log folder,
        report_data folder
        notify data folder
        :return:
        """
        keeping_days = 7
        cls.logger.info(f"Start doing house keeping, will only keep latest {keeping_days} data.")
        today_date = datetime.strptime(util.start_date, '%Y-%m-%d')
        target_date = today_date - timedelta(days=keeping_days)
        target_date_str = target_date.strftime('%Y-%m-%d')
        # clean the output folder
        data_folders = os.listdir(constants.output_base_dir)
        for data_folder in data_folders:
            full_path = constants.output_base_dir + '/' + data_folder
            if os.path.isdir(full_path) and data_folder < target_date_str:
                shutil.rmtree(full_path)
                cls.logger.info(f"Remove data folder {full_path}")

        # clean the datebase folder
        # new_seeds_2018-01-19, seeds_2018-01-19
        seeds_files = [file for file in os.listdir(constants.database_dir)
                       if file.startswith('seeds') or file.startswith('new_seeds')]
        for seed_file in seeds_files:
            seed_file_index = seed_file.rfind('_')
            if seed_file_index > -1:
                seed_date = seed_file[seed_file_index + 1:]
                if seed_date < target_date_str:
                    os.remove(constants.database_dir + '/' + seed_file)
                    cls.logger.info(f"Remove seed file {constants.database_dir + '/' + seed_file}")

        # log file
        # 2018-01-04_crawl.log
        log_files = os.listdir('log')
        for log_file in log_files:
            log_file_index = log_file.find('_')
            if log_file_index > -1:
                log_date = log_file[:log_file_index]
                if log_date < target_date_str:
                    os.remove('log/' + log_file)
                    cls.logger.info(f"Remove log file {'log/' + log_file}")

        # report_data folder
        report_datas = os.listdir(constants.report_data_dir)
        for report_data in report_datas:
            report_data_index = report_data.find('_')
            if report_data_index > -1:
                report_data_date = report_data[: report_data_index]
                if report_data_date < target_date_str:
                    os.remove(constants.report_data_dir + '/' + report_data)
                    cls.logger.info(f"Remove report data {constants.report_data_dir + '/' + report_data}")

        # notify data folder
        notify_datas = os.listdir(constants.notifies_dir)
        for notify_data in notify_datas:
            notify_data_index = notify_data.find('_')
            if notify_data_index > -1:
                notify_data_date = notify_data[: notify_data_index]
                if notify_data_date < target_date_str:
                    os.remove(constants.notifies_dir + '/' + notify_data)
                    cls.logger.info(f"Remove notify data {constants.notifies_dir + '/' + notify_data}")

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
            # generate char data
            cls._gen_char_data()
        else:
            cls.logger.warning(f"No need to generate report, because of the data is not newer than cache.")

        cls.house_keeping()
        cls.logger.info(f"Finish reporting.")


if __name__ == '__main__':
    ReportService.work()



