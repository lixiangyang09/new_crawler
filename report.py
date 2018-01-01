#!/usr/bin/env python
# encoding=utf8

from datetime import datetime, timedelta


class ReportService:

    @classmethod
    def gen_report(cls, date):
        previous_day = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)
        previous_day = previous_day.strftime('%Y-%m-%d')
        # check the existance of cache and data
        cls.house_cache_file = 'house_cache_' + previous_day
        cls.daily_cache_file = 'daily_cache_' + previous_day
        cls.down_cache_file = 'down_cache_' + previous_day

