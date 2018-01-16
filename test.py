
#!/usr/bin/env python
# encoding=utf8

import ast

from request import RequestService
from extract import ExtractService
from seeds import SeedsService
from store import FileService

# SeedsService.start()
#
#
# seed = SeedsService.get_template('content', 'index', 'lianjia')
#
# seed.url = 'https://bj.lianjia.com/ershoufang/haidian/pg1bp200ep600/'
#
# res = RequestService.request(seed)
#
# cont = ExtractService.extract(seed, res[2])
# print(cont)
#
# seed.url = 'https://cd.lianjia.com/ershoufang/jinniu/bp251ep10000/'
#
# res = RequestService.request(seed)
#
# cont = ExtractService.extract(seed, res[2])
# print(cont)

import util
import os
import ast
from store import FileService

# path = 'output/2018-01-01'
# files = os.listdir(path)
# for file in files:
#     data_str = FileService.load_file(path + '/' + file)
#     data = ast.literal_eval(data_str)
#     data['hash_code'] = util.get_hash('https://cd.lianjia.com/ershoufang/' + data['id'] + '.html')
#     FileService.save_file(path, file, data)
# a = (util.get_hash('https://bj.lianjia.com/ershoufang/101102422688.html'))
# print(a)
# input_file = input("input file full path")
# output_file = input("output file full path")
#
# with open(output_file, 'w') as output_handle:
#     with open(input_file) as input_handle:
#         for line in input_handle:
#             hash_code = util.get_line_hash(line)
#             tokens = line.strip().split(',')
#             if hash_code:
#                 output_line = util.get_hash(tokens[2]) + ',content,page,lianjia,' + tokens[2] + '\n'
#                 output_handle.write(output_line)
#
#
input_data = "{'type': 'house', 'city': 'beijing', 'status': '', 'id': '101102363010', 'price': '335', 'area': '37.8平米', 'unit_price': '88625', 'structure_info': '1室1厅', 'community_name': '红联南村', 'community_link': '/xiaoqu/1111027375144/', 'district': '海淀', 'subdistrict': '新街口', 'ring_loc': '二至三环', 'fav_count': '22', 'view_count': '12', 'room_count': '1室1厅1厨1卫', 'listed_time': '2017-12-02', 'deal_period': '', 'source': 'lianjia', 'hash_code': '9dc21740'}"
res = ast.literal_eval(input_data)
print(res)