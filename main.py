#!/usr/bin/env python
# encoding=utf8
import util
import os
import shutil
import logging
from crawl import CrawlService
from datetime import datetime
from seeds import SeedsService
from proxy import ProxyService
import bj_jianwei


if not os.path.exists('log'):
    os.mkdir('log')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)15s[%(lineno)4d] %(levelname)8s %(thread)d %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    handlers=[logging.FileHandler(
                        filename="./log/" + util.start_date + "_crawl.log", # str(datetime.now()).replace(" ", "_").replace(":", "_") + "_crawler.log",
                        mode='w', encoding="utf-8")])
logger = logging.getLogger(__name__)


def main():
    # init folder
    output_dir = util.get_output_data_dir()
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    # init services
    SeedsService.start()
    ProxyService.start()

    # crawl data
    CrawlService.start()

    # stop services
    ProxyService.stop()
    SeedsService.stop()
    # generate data report

    # generate proxy report


if __name__ == '__main__':
    main()
    bj_jianwei.crawl_data()
