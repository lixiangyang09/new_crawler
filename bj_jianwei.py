#!/usr/bin/env python
# encoding=utf8

from request import RequestService
from store import FileService
from lxml import html
import util
import os
import datetime


def extract_data(monthly_header, monthly_data, daily_title, daily_header, daily_data):
    final_data = []

    monthly_res = dict()
    mid = len(monthly_header) / 2

    monthly_res['title'] = daily_title[1]
    current_session = '区县'
    monthly_res[current_session] = list()
    for i in range(1, int(mid) - 3):
        key = monthly_header[i]. \
                    replace('\xa0', ''). \
                    replace('\u3000', '',). \
                    replace('(', ''). \
                    replace('[', ''). \
                    replace('m', '')
        monthly_res[current_session].append(key)

    for i in range(int(mid) + 1, len(monthly_header) - 3):
        key = monthly_header[i]. \
                    replace('\xa0', ''). \
                    replace('\u3000', '',). \
                    replace('(', ''). \
                    replace('[', ''). \
                    replace('m', '')
        monthly_res[current_session].append(key)

    monthly_res['套数'] = list()
    monthly_res['成交面积'] = list()
    for i in range(0, len(monthly_data)):
        if int(i / 10) % 2 == 0:
            target = monthly_res['套数']
        else:
            target = monthly_res['成交面积']
        target.append(monthly_data[i])

    final_data.append(monthly_res)

    for i in [0, 1, 2]:
        tmp = dict()
        tmp['title'] = daily_title[i]
        for j, k in [(0, 0), (1, 1), (3, 2), (4, 3)]:
            tmp[daily_header[j + i * 6].strip().replace('：', '').replace('(m', '')] = daily_data[k + i * 4]
        final_data.append(tmp)
    return final_data


def save_data(output_data):
    output_path = os.path.join(util.get_output_base_dir(), 'jianwei')
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    file_name = datetime.datetime.now().strftime("%Y-%m-%d")
    FileService.save_file(output_path, file_name, output_data)


def crawl_data():
    status_code, url, data = RequestService.normal_request('http://210.75.213.188/shh/portal/bjjs/index.aspx')

    bj_tree = html.fromstring(data)

    monthly_header = bj_tree.xpath('//div[@class="tContent"]/ul/li[2]//th/text()')
    monthly_data = bj_tree.xpath('//div[@class="tContent"]/ul/li[2]//td/text()')
    daily_title = bj_tree.xpath('//table[@class="tjInfo"]/thead/tr/th/text()')
    daily_header = bj_tree.xpath('//table[@class="tjInfo"]/tbody/tr/th/text()')
    daily_data = bj_tree.xpath('//table[@class="tjInfo"]/tbody/tr/td/text()')

    save_data(extract_data(monthly_header, monthly_data, daily_title, daily_header, daily_data))


if __name__ == '__main__':
    crawl_data()
