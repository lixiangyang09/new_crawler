#!/usr/bin/env python
# encoding=utf8


import logging
import util
from seeds import SeedsService
from store import StoreService
from proxy import Proxy, ProxyService
import re


class ProcessorInterface(object):
    def __init__(self, indicator):
        self.indicator = indicator
        logger_name = "Processor_" + self.indicator
        self.logger = logging.getLogger(logger_name)

    def process_page_data(self, data):
        raise NotImplementedError

    def process_index_data(self, data):
        # mainly index: urls, page: urls
        if data and 'source' in data:
            source = data['source']
            del data['source']
        else:
            self.logger.warning(f"Empty data in process_index_data or 'source' not in data.")
            return
        for target, urls in data.items():
            for url in urls:
                hash_key = util.get_hash(url)
                seed = SeedsService.get_template(self.indicator, target, source)
                seed.url = url
                seed.hash_key = hash_key
                seed.source = source
                SeedsService.put(seed)


class ContentProcessor(ProcessorInterface):
    def __init__(self):
        super(ContentProcessor, self).__init__('content')

    def process_page_data(self, data):
        StoreService.save_data(data)


class ProxyProcessor(ProcessorInterface):
    ip_pattern = re.compile(r'(\d{1,3}).(\d{1,3}).(\d{1,3}).(\d{1,3})')

    def __init__(self):
        super(ProxyProcessor, self).__init__('proxy')

    def validate_ip(self, ip):
        match = self.ip_pattern.match(ip)
        if match:
            return True
        else:
            return False

    def edit_http_type(self, data):
        h = 'http'
        hs = 'https'
        sock = 'sock5'
        res = ''
        data = data.lower()
        if data == 'http':
            res = 'http'
        elif data == h + "," + hs or hs + "," + h:
            res = hs
        elif data == sock:
            pass
        return res

    def process_page_data(self, data):
        # mainly ips, ports, types
        ips = data['ip']
        ports = data['port']
        types = data.get('type', None)
        if types is None:
            types = ['type'] * len(ips)
        for k in zip(ips, ports, types):
            if self.validate_ip(k[0]):
                proxy_type = self.edit_http_type(k[2])
                hash_key = util.get_hash(k[0] + ":" + k[1])
                new_proxy_instance = Proxy(hash_key, k[0], k[1], proxy_type)
                ProxyService.put(new_proxy_instance)


class ProcessService:
    content_processor = ContentProcessor()
    proxy_processor = ProxyProcessor()

    @classmethod
    def process(cls, seed, data):
        if seed.seed_type == 'proxy':
            if seed.seed_target == 'index':
                cls.proxy_processor.process_index_data(data)
            elif seed.seed_target == 'page':
                cls.proxy_processor.process_page_data(data)
        elif seed.seed_type == 'content':
            if seed.seed_target == 'index':
                cls.content_processor.process_index_data(data)
            elif seed.seed_target == 'page':
                cls.content_processor.process_page_data(data)

