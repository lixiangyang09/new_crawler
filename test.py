
#!/usr/bin/env python
# encoding=utf8


# from request import RequestService
# from extract import ExtractService
# from seeds import SeedsService
#
# SeedsService.start()
#
#
# seed = SeedsService.get_template('content', 'page', 'lianjia')
#
# seed.url = 'https://cd.lianjia.com/ershoufang/106100750158.html'
#
# res = RequestService.request(seed)
#
# cont = ExtractService.extract(seed, res[2])


import util
import os
import ast
from store import FileService

path = 'output/2018-01-01'
files = os.listdir(path)
for file in files:
    data_str = FileService.load_file(path + '/' + file)
    data = ast.literal_eval(data_str)
    data['hash_code'] = util.get_hash('https://cd.lianjia.com/ershoufang/' + data['id'] + '.html')
    FileService.save_file(path, file, data)


