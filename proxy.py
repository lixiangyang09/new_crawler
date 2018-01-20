#!/usr/bin/env python
# encoding=utf8
import queue
from datetime import datetime, timedelta
from threading import Lock
import logging
from queue import Empty
import os
from sync_set import SyncSet
import util
import constants


class Proxy:
    def __init__(self, hash_code, ip, port, proxy_type, last_verify=str(datetime.now())):
        self.ip = ip  # i.e. 127.0.0.1
        self.port = port  # i.e. 5560
        self.type = proxy_type  # i.e. 'http', 'https'
        self.hash_code = hash_code  # the hash generated by utils.helper.gen_hash function
        self.last_verify = last_verify  # the date string, "2017-09-21 13:32:21.121321"

    def __repr__(self):
        return str(self.hash_code) + "," + str(self.ip) + "," + str(self.port)\
               + "," + str(self.type) + "," + str(self.last_verify)


class Status:
    def __init__(self):
        self.fly = True
        self.invoke_count = 1
        self.invoke_history = [datetime.now()]

    def put(self):
        self.fly = False

    def get(self):
        self.fly = True
        self.invoke_count += 1
        self.invoke_history.append(datetime.now())


class StatusService:
    _status = dict()

    @classmethod
    def get(cls, hash_code):
        if hash_code in cls._status:
            cls._status[hash_code].get()
        else:
            cls._status[hash_code] = Status()

    @classmethod
    def put(cls, hash_code):
        if hash_code in cls._status:
            cls._status[hash_code].put()

    @classmethod
    def report(cls):
        total = 0
        work = 0
        worked = 0
        work_count = dict()
        worked_count = dict()
        for hash_code, status in cls._status.items():
            total += 1
            if status.fly:
                if len(status.invoke_history) > 1:
                    worked += 1
                    worked_count_res = worked_count.get(status.invoke_count, 0)
                    worked_count[status.invoke_count] = worked_count_res + 1
            else:
                work += 1

            for ind in range(0, status.invoke_count):
                work_count_res = work_count.get(ind, {'count': 0, 'total': 0})
                work_count_res['count'] += 1
                if ind > 0:
                    work_count_res['total'] += (status.invoke_history[ind] - status.invoke_history[ind-1]).seconds
                work_count[ind] = work_count_res

        report_str = f'proxy使用情况:\n ' \
                     f'   总量:{str(total)}\n' \
                     f'   可用:{str(work)}\n' \
                     f' 曾经可用： {str(worked)}\n'
        for count, statistic in worked_count.items():
            report_str += f'   调用{str(count + 1)}次后, {str(statistic)} 不可再使用\n'

        for count, statistic in work_count.items():
            local_count = statistic['count']
            local_avg = statistic['total'] / local_count
            report_str += f'调用{str(count + 1)}次, 数量: {str(local_count)}, 平均间隔(s): {str(local_avg)}\n'
        return report_str


class ProxyService:
    start_proxy_count = 0
    stop_proxy_count = 0
    # the process that story all current crawled proxies
    proxy_all = {}
    # proxies that used in current program
    proxies = queue.Queue(0)  # process.Proxy instance
    # already exist
    appeared = SyncSet()
    # the lock
    lock = Lock()

    logger = logging.getLogger(__name__)

    @classmethod
    def _update_proxy(cls, proxy_instance, last_time=None):
        if last_time is None:
            last_time = datetime.strptime(proxy_instance.last_verify, '%Y-%m-%d %H:%M:%S.%f')
        current_time = datetime.now()
        # the process can't used in two days, delete it
        if (current_time - last_time) > timedelta(days=2):
            del cls.proxy_all[proxy_instance.hash_code]
            cls.logger.info("delete proxy: " + proxy_instance.to_string())
        else:
            # update process
            proxy_instance.last_verify = str(last_time)
            cls.proxy_all[proxy_instance.hash_code] = proxy_instance

    @classmethod
    def get(cls):
        try:
            proxy_instance = cls.proxies.get(block=True, timeout=5)
            StatusService.get(proxy_instance.hash_code)
            cls.appeared.remove(proxy_instance.hash_code)
        except Empty:
            cls.logger.warning("no available proxy resource, please reduce the crawl seeds or add proxy resource.")
            proxy_instance = None
        return proxy_instance

    @classmethod
    def put(cls, proxy_instance):
        """
        put the proxy back and update the last available time
        :param proxy_instance:
        :return:
        """
        if proxy_instance is None:
            return
        with cls.lock:
            if not cls.appeared.exist(proxy_instance.hash_code):
                cls.logger.info(f"Found new proxy, {proxy_instance}")
                cls._update_proxy(proxy_instance, datetime.now())
                cls.proxies.put_nowait(proxy_instance)
                cls.proxy_all[proxy_instance.hash_code] = proxy_instance
                cls.appeared.add(proxy_instance.hash_code)

                StatusService.put(proxy_instance.hash_code)

    @classmethod
    def start(cls):
        proxy_raw_count = 0
        if os.path.exists(constants.raw_proxy_file):
            with open(constants.raw_proxy_file) as f:  # hash, process, date
                for line in f:
                    cleaned_line = line.rstrip('\n')
                    if cleaned_line:
                        parts = cleaned_line.split(":")
                        hash_code = util.gen_hash(cleaned_line)
                        proxy_instance = Proxy(hash_code, parts[0], parts[1], "http")
                        cls.put(proxy_instance)
                        proxy_raw_count += 1
            if not cls.proxy_all:
                cls.logger.warning(f"No proxy after {constants.raw_proxy_file} function")
            else:
                cls.logger.info(f"Totally load {str(proxy_raw_count)} raw proxies")
        else:
            cls.logger.warning(f"can't find {constants.raw_proxy_file} file")
        proxy_db_count = 0
        if os.path.exists(constants.proxy_file):
            with open(constants.proxy_file) as f:  # hash, process, date
                for line in f:
                    cleaned_line = line.rstrip('\n')
                    if cleaned_line:
                        parts = cleaned_line.split(",")
                        proxy_instance = Proxy(parts[0], parts[1], parts[2], parts[3], parts[4])
                        cls.put(proxy_instance)
                        proxy_db_count += 1
            if not cls.proxy_all:
                cls.logger.warning(f"No proxy after {constants.proxy_file} function")
            else:
                cls.logger.info(f"Totally load {str(proxy_db_count)} proxies from {constants.proxy_file}")
        else:
            cls.logger.warning(f"can't find {constants.proxy_file} file")
        cls.start_proxy_count += proxy_raw_count + proxy_db_count

    @classmethod
    def stop(cls):
        with open(constants.proxy_file, 'w') as f:
            for proxy in cls.proxy_all.values():
                output = str(proxy) + "\n"
                f.write(output)
                cls.stop_proxy_count += 1
        cls.logger.info(f"Write back {str(cls.stop_proxy_count)} proxies.")

        email_subject = f"{util.start_date} new crawler proxy report"
        msg = f"Start with {cls.start_proxy_count} proxies, stop with {cls.stop_proxy_count} proxies.\n"
        msg += f"Start time {util.start_time}, stop time {datetime.now()}"
        msg += cls.generate_report()
        util.send_mail("562315079@qq.com", "qlwhrvzayytcbche",
                       ["562315079@qq.com"],
                       email_subject, msg)
        cls.logger.info(f"Finish sending proxy report.")

    @classmethod
    def generate_report(cls):
        return StatusService.report()
