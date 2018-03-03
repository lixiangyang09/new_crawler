#!/usr/bin/env python
# encoding=utf8
import os

data_file_suffix = "_report_data.tar.gz"
report_data_dir = 'report_data'
conf_path = "database/configuration.yml"

new_seeds_file = './database/new_seeds'

raw_proxy_file = './database/proxy_raw.db'

proxy_file = './database/proxy.db'

notifies_dir = './notify_data'

output_base_dir = 'output'

database_dir = 'database'

seeds_file = './database/seeds'

chart_data_dir = 'chart_data'

tmp_data_dir = 'tmp'

jianwei_data_dir = 'jianwei'

monthly_detail_file = os.path.join(chart_data_dir, 'monthly_detail')
monthly_overview_file = os.path.join(chart_data_dir, 'monthly_overview')
daily_check_file = os.path.join(chart_data_dir, 'daily_check')
daily_signed_file = os.path.join(chart_data_dir, 'daily_signed')

# output_base_dir = '/Users/dev/lianjia/result'
#
# database_dir = '/Users/dev/lianjia/result'
#
# seeds_file = '/Users/dev/lianjia/result/seeds'
