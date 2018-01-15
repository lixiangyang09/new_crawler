
#!/usr/bin/env python
# encoding=utf8


from request import RequestService
from extract import ExtractService
from seeds import SeedsService
from store import FileService

SeedsService.start()


seed = SeedsService.get_template('content', 'index', 'lianjia')

seed.url = 'https://bj.lianjia.com/ershoufang/haidian/pg1bp200ep600/'

res = RequestService.request(seed)

cont = ExtractService.extract(seed, res[2])
print(cont)

seed.url = 'https://cd.lianjia.com/ershoufang/jinniu/bp251ep10000/'

res = RequestService.request(seed)

cont = ExtractService.extract(seed, res[2])
print(cont)

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
