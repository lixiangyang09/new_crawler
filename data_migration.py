#!/usr/bin/env python
# encoding=utf8

import os
from store import FileService
import ast
import util
import shutil

dir_source_base = '/Users/dev/python_programs/new_crawler/'

dir_input = dir_source_base + 'report_data'

dir_tmp = 'report_tmp'

dir_database = 'database'

dir_output = 'output'

dir_backup_output = '../migration_backup_output'
dir_backup_database = '../migration_backup_database'


def clean_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)


def get_file_line_count(file):
    count = 0
    if os.path.exists(file):
        with open(file) as input_file:
            for line in input_file:
                if line.strip():
                    count += 1
    return count

# step 1. backup the data of new_crawl


if os.path.exists(dir_backup_output):
    shutil.rmtree(dir_backup_output)
if os.path.exists(dir_backup_database):
    shutil.rmtree(dir_backup_database)

if os.path.exists(dir_output):
    shutil.copytree(dir_output, dir_backup_output)
if os.path.exists(dir_database):
    shutil.copytree(dir_database, dir_backup_database)


# fresh the data from crawl and put them to new_crawl folder


clean_dir(dir_tmp)
data_file_suffix = "_report_data.tar.gz"

# get packed files
files = os.listdir(dir_input)
packed_files = [dir_input + '/' + file for file in files]

for file in packed_files:
    print(f"Processing file {file}")
    # get date
    filename = os.path.basename(file)
    end_index = filename.rindex(data_file_suffix)
    date = filename[:end_index]

    # unpack file
    print('unpack\n')
    FileService.unpack_file(file, dir_tmp)
    # fresh output folder
    data_files_tmp = os.listdir(dir_tmp + '/' + date + '/' + date)
    data_files = [dir_tmp + '/' + date + '/' + date + '/' + file for file in data_files_tmp if '-' in file]

    file_count_source = len(data_files)
    if os.path.exists(dir_output + '/' + date):
        file_count_target = len(os.listdir(dir_output + '/' + date))
    else:
        file_count_target = 0

    for data_file in data_files:
        data_str = FileService.load_file(data_file)
        data_obj = ast.literal_eval(data_str)
        data_obj['hash_code'] = util.get_hash('https://bj.lianjia.com/ershoufang/'
                                              + data_obj['id'] + '.html')
        FileService.save_file(dir_output + '/' + date, os.path.basename(data_file), data_obj)
    print('After fresh data file to output folder')
    # fresh seed file
    seed_file = dir_tmp + '/' + date + '/' + 'content_page_seed_list'
    target_seed_file = dir_database + '/' + 'seeds_' + date
    if not os.path.exists(dir_database):
        os.mkdir(dir_database)
    seed_count_source = get_file_line_count(seed_file)
    seed_count_target = get_file_line_count(target_seed_file)

    with open(target_seed_file, 'a') as output:
        with open(seed_file) as f:
            for line in f:
                line = line.strip()
                hash_code = util.get_line_hash(line)
                if hash_code:
                    tokens = line.split(',')
                    new_hash = util.get_hash(tokens[2])
                    output.write(f"{new_hash},content,page,lianjia,{tokens[2]}\n")
    print("after merge seed file")
    # fresh new_seed file
    new_seed_file = dir_tmp + '/' + date + '/' + 'new_content_page_seed_list'
    target_new_seed_file = dir_database + '/' + 'new_seeds_' + date

    new_seed_count_source = get_file_line_count(new_seed_file)
    new_seed_count_target = get_file_line_count(target_new_seed_file)

    with open(target_new_seed_file, 'a') as output:
        with open(new_seed_file) as f:
            for line in f:
                line = line.strip()
                hash_code = util.get_line_hash(line)
                if hash_code:
                    tokens = line.split(',')
                    new_hash = util.get_hash(tokens[2])
                    output.write(f"{new_hash},content,page,lianjia,{tokens[2]}\n")
    shutil.rmtree(dir_tmp + '/' + date)
    print('after merge new seed file')

    file_count_res = len(os.listdir(dir_output + '/' + date))
    seed_count_res = get_file_line_count(target_seed_file)
    new_seed_count_res = get_file_line_count(target_new_seed_file)
    # pack data

    if file_count_source + file_count_target != file_count_res:
        print(f"{file_count_source} + {file_count_target} = {file_count_source + file_count_target} == {file_count_res}")
        continue
    if seed_count_source + seed_count_target != seed_count_res:
        print(f"{seed_count_source} + {seed_count_target} = {seed_count_source + seed_count_target} == {seed_count_res}")
        continue
    if new_seed_count_source + new_seed_count_target != new_seed_count_res:
        print(f"{new_seed_count_source} + {new_seed_count_target} = {new_seed_count_source + new_seed_count_target} "
              f"== {new_seed_count_res}")
        continue

    print(f"source + target == result\n")

    # tar_file = dir_convert + "/" + date + data_file_suffix
    # if os.path.exists(tar_file):
    #     os.remove(tar_file)
    # dir_convert_tmp = dir_convert + '/' + date
    # if os.path.exists(dir_convert_tmp):
    #     shutil.rmtree(dir_convert_tmp)
    # os.mkdir(dir_convert_tmp)
    #
    # shutil.copyfile(target_seed_file, dir_convert_tmp + "/" + os.path.basename(target_seed_file))
    # shutil.copyfile(target_new_seed_file, dir_convert_tmp + "/" + os.path.basename(target_new_seed_file))
    #
    # shutil.copytree(dir_output + '/' + date, dir_convert_tmp + "/" + date)
    # # pack
    # current_dir = os.getcwd()
    # os.chdir(dir_convert)
    # FileService.pack_folder(os.path.basename(tar_file), date, True)
    # os.chdir(current_dir)


