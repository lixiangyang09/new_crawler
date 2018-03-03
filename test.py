
#!/usr/bin/env python
# encoding=utf8

import ast

from request import RequestService
from extract import ExtractService
from seeds import SeedsService
from store import FileService
from lxml import html

'//*[@id="introduction"]/div/div/div[2]/div[2]/ul/li[1]/span[2]'

list_time = '(//div[@class="transaction"]//span)[2]/text()'

status_code, url, data = RequestService.normal_request('https://cd.lianjia.com/ershoufang/106100231725.html')

bj_tree = html.fromstring(data)

list_tt = bj_tree.xpath(list_time)

status_code, url, data = RequestService.normal_request('http://210.75.213.188/shh/portal/bjjs/index.aspx')

bj_tree = html.fromstring(data)
xp = '/html/body/div[4]/div[4]/div[7]/div/ul/li/div[3]/table/tbody/tr[1]/th'


xx_test = '//table[@class="tjInfo"]/tbody/tr/th/text()'
xx_test_d = '//table[@class="tjInfo"]/tbody/tr/td/text()'

test = bj_tree.xpath(xx_test)
test_d = bj_tree.xpath(xx_test_d)

tokens = xp.split('/')
xxp = '/'
for key in tokens:
    xxp += key
    print(xxp)
    dd = bj_tree.xpath(xxp)
    print(dd)

bj_data = bj_tree.xpath(xp)
print(bj_data)

status_code, url, data = RequestService.normal_request('http://www.bjjs.gov.cn/bjjs/fwgl/fdcjy/fwjy/index.shtml')
root = html.fromstring(data)

# content = '//div[@name="内容"]'
# content_ele = root.xpath(content)
# node1 = root.findtext('商品房数据统计')
# for ele in node1.iter():
#     print("%s - %s" % (ele.tag, ele.text))
#
# for element in content_ele:
#     for ele in element.iter():
#         print("%s - %s" % (ele.tag, ele.text))
content = '//tbody'
node2 = root.findall("tbody")
for ele in node2:
    print("%s - %s" % (ele.tag, ele.text))
node3 = root.xpath(".//tbody")
for element in node3:
        for ele in element.iter():
                print("%s - %s" % (ele.tag, ele.text))
print("finished")



status = '2017.10.21 链家成交'

down_time = status.strip().split(' ')[0].replace('.', '-')

SeedsService.start()


seed = SeedsService.get_template('content', 'page', 'lianjia')

seed.url = 'https://bj.lianjia.com/chengjiao/101101904861.html'

res = RequestService.request(seed)

cont = ExtractService.extract(seed, res[2])
print(cont)

# seed2 = SeedsService.get_template('proxy', 'index', 'ip181')
#
# seed2.url = 'http://www.ip181.com/daili/1.html'
# res = RequestService.request(seed2)
#
# cont = ExtractService.extract(seed2, res[2])
# print(cont)

seed3 = SeedsService.get_template('proxy', 'page', 'ip181')

seed3.url = 'http://www.ip181.com/daili/1.html'
res = RequestService.request(seed3)

cont = ExtractService.extract(seed3, res[2])
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
#     data['hash_code'] = util.gen_hash('https://cd.lianjia.com/ershoufang/' + data['id'] + '.html')
#     FileService.save_file(path, file, data)
# a = (util.gen_hash('https://bj.lianjia.com/ershoufang/101102422688.html'))
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
#                 output_line = util.gen_hash(tokens[2]) + ',content,page,lianjia,' + tokens[2] + '\n'
#                 output_handle.write(output_line)
#
#
input_data = "{'type': 'house', 'city': 'beijing', 'status': '', 'id': '101102363010', 'price': '335', 'area': '37.8平米', 'unit_price': '88625', 'structure_info': '1室1厅', 'community_name': '红联南村', 'community_link': '/xiaoqu/1111027375144/', 'district': '海淀', 'subdistrict': '新街口', 'ring_loc': '二至三环', 'fav_count': '22', 'view_count': '12', 'room_count': '1室1厅1厨1卫', 'listed_time': '2017-12-02', 'deal_period': '', 'source': 'lianjia', 'hash_code': '9dc21740'}"
res = ast.literal_eval(input_data)
print(res)




status_code, url, data = RequestService.normal_request('http://www.bjjs.gov.cn/bjjs/fwgl/fdcjy/fwjy/index.shtml')
root = html.fromstring(data)
for element in root.iter():
     print("%s - %s" % (element.tag, element.text))

print("finished")




