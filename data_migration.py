#!/usr/bin/env python
# encoding=utf8

import os
from store import FileService
import ast
import util
import shutil
from datetime import datetime, date, timedelta
import traceback



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


def fresh_hash():
    dir_source_base = '/Users/dev/lianjia/'

    dir_beijing = dir_source_base + 'beijing'

    dir_chengdu = dir_source_base + 'chengdu'

    dir_result = dir_source_base + 'result'

    dir_tmp = dir_source_base + 'tmp'

    dir_seed = dir_source_base + 'seeds/beijing'

    clean_dir(dir_result)

    data_file_suffix = "_report_data.tar.gz"

    # process beijing

    try:
        # get packed files
        files = os.listdir(dir_beijing)
        packed_files = [dir_beijing + '/' + file for file in files if file.endswith(data_file_suffix)]
        test = ['/Users/dev/lianjia/beijing/2018-01-07_report_data.tar.gz']
        for file in sorted(packed_files):
            print(f"Processing file {file}")
            # get date
            filename = os.path.basename(file)
            end_index = filename.rindex(data_file_suffix)
            date = filename[:end_index]

            # unpack file
            clean_dir(dir_tmp)
            FileService.unpack_file(file, dir_tmp)
            # fresh output folder
            data_files_tmp = os.listdir(dir_tmp + '/' + date + '/' + date)
            data_files = [dir_tmp + '/' + date + '/' + date + '/' + file for file in data_files_tmp if '-' in file]

            if os.path.exists(dir_tmp + '/' + date + '/content_page_seed_list'):
                shutil.copy(dir_tmp + '/' + date + '/content_page_seed_list', dir_seed + '/seeds_' + date)

            file_count_source = len(data_files)
            print(f'file count: {file_count_source}')
            print('\n')
            for data_file in data_files:

                data_str = FileService.load_file(data_file)
                data_obj = ast.literal_eval(data_str)
                data_obj['city'] = 'bj'
                data_obj['hash_code'] = util.gen_hash('https://bj.lianjia.com/ershoufang/'
                                                      + data_obj['id'] + '.html')
                FileService.save_file(dir_result + '/' + date, os.path.basename(data_file), data_obj)

        # mv chengdu
        dirs = os.listdir(dir_chengdu)
        chengdu_date = [x for x in dirs if os.path.isdir(dir_chengdu + '/' + x)]

        for date in sorted(chengdu_date):
            chengdu_dir = dir_chengdu + '/' + date
            print(chengdu_dir)
            files = os.listdir(chengdu_dir)
            file_count = len(files)
            print(f"file count {file_count}")
            print('\n')
            for data_file in files:
                file_name = os.path.basename(data_file)
                data_str = FileService.load_file(chengdu_dir + '/' + data_file)
                if os.path.exists(dir_result + '/' + date + '/' + file_name):
                    print(f"Already exist {file_name} in {dir_result + '/' + date}")
                    file_name = str(util.get_uuid())
                FileService.save_file(dir_result + '/' + date, file_name, data_str)
        for res_folder in os.listdir(dir_result):
            res_files = os.listdir(dir_result + '/' + res_folder)
            res_file_count = len(res_files)
            print(f"Result file count {res_folder}: {res_file_count}")
    except BaseException:
        print(data_file)
        print(data_str)
        print(traceback.print_exc())


def csv_to_data():
    csv_file = '/Users/dev/Downloads/2018-01-07.csv'
    output_base = '/Users/dev/lianjia'
    file_date = '2018-01-07'

    output_folder = output_base + '/result/' + file_date
    seed_file = open(output_base + '/seeds/beijing/seeds_' + file_date, 'w')

    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder)

    {'type': 'house', 'city': 'bj', 'status': '', 'id': '101101779478', 'price': '610', 'area': '210.97平米',
     'unit_price': '28915', 'structure_info': '3室2厅', 'community_name': '天通苑北二区',
     'community_link': '/xiaoqu/1111027380040/', 'district': '昌平', 'subdistrict': '天通苑', 'ring_loc': '五至六环',
     'fav_count': '68', 'view_count': '13', 'room_count': '3室2厅1厨2卫', 'listed_time': '2017-07-05', 'deal_period': '',
     'source': 'lianjia', 'hash_code': '47212741'}

    contents = FileService.load_file(csv_file, 'utf_8_sig')
    lines = contents.splitlines()
    count = 0
    for line in lines:
        count += 1
        if count == 1:
            continue
        else:
            line = line.strip()
            if line:
                tokens = line.split(',')
                if '成交' in tokens[12]:
                    continue
                data = {'id': tokens[0],
                        'type': 'house',
                        'price': tokens[1],
                        'area': tokens[2] + '平米',
                        'unit_price': str(int(float(tokens[3])*10000)),
                        'community_link': '/xiaoqu/' + tokens[4] + '/',
                        'community_name': tokens[5],
                        'district': tokens[6],
                        'subdistrict': tokens[7],
                        'city': tokens[8],
                        'source': tokens[9],
                        'fav_count': tokens[10],
                        'view_count': tokens[11],
                        'status': tokens[12],
                        'deal_period': tokens[13],
                        'hash_code': util.gen_hash('https://bj.lianjia.com/ershoufang/' + tokens[0] + '.html')}
                FileService.save_file(output_folder, str(util.get_uuid()), data)
                seed_file.write(f"{data['hash_code']},lianjia,"
                                f"https://bj.lianjia.com/ershoufang/{tokens[0]}.html\n")
    seed_file.close()


def rename_folder():
    data_folder = '/Users/dev/lianjia/result'
    folders = sorted(os.listdir(data_folder), reverse=True)
    for folder in folders:
        if folder < '2017-11-27':
            folder_date = datetime.strptime(folder, '%Y-%m-%d')
            target_date = folder_date + timedelta(days=1)
            target_folder = target_date.strftime('%Y-%m-%d')
            os.rename(data_folder + '/' + folder,
                      data_folder + '/' + target_folder)
            os.rename('/Users/dev/lianjia/seeds/beijing/seeds_' + folder,
                      '/Users/dev/lianjia/seeds/beijing/seeds_' + target_folder)
            print(f'Rename {folder} to {target_folder}')


def merge_seeds():
    seeds_base_dir = '/Users/dev/lianjia/seeds/'
    result_dir = '/Users/dev/lianjia/result/'
    bj_seeds = [seed for seed in os.listdir(seeds_base_dir + 'beijing') if seed.startswith('seeds')]
    cd_seeds = os.listdir(seeds_base_dir + 'chengdu')
    for bj_seed in bj_seeds:
        print(bj_seed)
        with open(result_dir + bj_seed, 'w') as out_handle:
            with open(seeds_base_dir + 'beijing/' + bj_seed) as in_handle:
                for line in in_handle:
                    if util.get_line_hash(line):
                        url = line.strip().split(',')[2]
                        res = util.gen_hash(url) + ',content,page,lianjia,' + url + '\n'
                        out_handle.write(res)
            if bj_seed in cd_seeds:
                with open(seeds_base_dir + 'chengdu/' + bj_seed) as in_handle:
                    for line in in_handle:
                        if util.get_line_hash(line):
                            out_handle.write(line)


if __name__ == '__main__':
    #fresh_hash()
    # csv_to_data()
    #rename_folder()
    merge_seeds()


