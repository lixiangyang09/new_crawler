#!/usr/bin/env python
# encoding=utf8

import os
from store import FileService
import ast
import util
import shutil


dir_input = 'report_data'

dir_tmp = '/tmp/report_tmp'

dir_database = '/tmp/database'

dir_output = '/tmp/output'

if os.path.exists(dir_tmp):
    shutil.rmtree(dir_tmp)
os.makedirs(dir_tmp)

if os.path.exists(dir_database):
    shutil.rmtree(dir_database)
os.makedirs(dir_database)

if os.path.exists(dir_output):
    shutil.rmtree(dir_output)
os.makedirs(dir_output)

# get packed files
files = os.listdir(dir_input)
packed_files = [dir_input + '/' + file for file in files]

for file in packed_files:
    print(f"Processing file {file}")
    # get date
    filename = os.path.basename(file)
    end_index = filename.rindex("_report_data.tar.gz")
    date = filename[:end_index]

    # unpack file
    FileService.unpack_file(file, dir_tmp)
    # fresh output folder
    data_files_tmp = os.listdir(dir_tmp + '/' + date + '/' + date)
    data_files = [dir_tmp + '/' + date + '/' + date + '/' + file for file in data_files_tmp]
    for data_file in data_files:
        data_str = FileService.load_file(data_file)
        data_obj = ast.literal_eval(data_str)
        data_obj['hash_code'] = util.get_hash('https://bj.lianjia.com/ershoufang/'
                                              + data_obj['id'] + '.html')
        FileService.save_file(dir_output + '/' + date, os.path.basename(data_file), data_obj)

    # fresh seed file
    seed_file = dir_tmp + '/' + date + '/' + 'content_page_seed_list'
    target_seed_file = dir_database + '/' + 'seeds_' + date
    if not os.path.exists(dir_database):
        os.mkdir(dir_database)

    with open(target_seed_file, 'w') as output:
        with open(seed_file) as f:
            for line in f:
                line = line.strip()
                hash_code = util.get_line_hash(line)
                if hash_code:
                    tokens = line.split(',')
                    new_hash = util.get_hash(tokens[2])
                    output.write(f"{new_hash},content,page,lianjia,{tokens[2]}\n")

    # fresh new_seed file
    new_seed_file = dir_tmp + '/' + date + '/' + 'content_page_seed_list_new'
    target_new_seed_file = dir_database + '/' + 'new_seeds_' + date

    with open(target_new_seed_file, 'w') as output:
        with open(new_seed_file) as f:
            for line in f:
                line = line.strip()
                hash_code = util.get_line_hash(line)
                if hash_code:
                    tokens = line.split(',')
                    new_hash = util.get_hash(tokens[2])
                    output.write(f"{new_hash},content,page,lianjia,{tokens[2]}\n")
    shutil.rmtree(dir_tmp + '/' + date)
