#!/usr/bin/env python
# encoding=utf8

from config import ConfigService
import util
from sync_set import SyncSet
from copy import deepcopy
import os
import queue
from threading import Lock
import logging


class Field:
    def __init__(self, name, xpath, required, udf):
        self.name = name
        self.xpath = xpath
        self.required = required
        self.udf = udf

    def __repr__(self):
        return f"Field, name: {self.name}, xpath: {self.xpath}" \
               f" required: {self.required}, udf: {self.udf}"


def parse_fields(data):
    fields = dict()
    if data is None or not data:
        return fields
    for (key, value) in data.items():
        field = parse_field(key, value)
        fields[key] = field
    return fields


def parse_field(name, data):
    if data is None:
        return None
    elif isinstance(data, dict):
        xpath = data.get("xpath")
        if isinstance(xpath, str):
            xpath = [xpath]
        required = data.get('required', False)
        udf = data.get("udf", None)
        if isinstance(udf, str):
            udf = {'func': udf}
        return Field(name, xpath, required, udf)
    elif isinstance(data, str):
        return Field(name, [data], False, None)


class Seed:
    def __init__(self, seed_type_source="",
                 seed_type_target="",
                 url="",
                 source="",
                 loadJS=False,
                 validate=[],
                 fields={},
                 hash_key=""):
        self.seed_type = seed_type_source
        self.seed_target = seed_type_target
        self.url = url
        self.source = source
        self.loadJS = loadJS
        self.validate = validate
        self.fields = fields
        self.hash_key = hash_key

    def update_hash(self):
        self.hash_key = util.get_hash(self.url)

    def __repr__(self):
        return self.seed_type + ',' + self.seed_target + ',' + self.url


class SeedsService:
    # template(seed_source_type, seed_target_source, source) = wanted_template
    template = dict()
    # seeds(seed_source_type, seed_target_source)
    config_seeds = queue.Queue(0)
    # work queue
    work_queue = queue.Queue(0)
    # Seed processed until now
    current_seeds = SyncSet()
    # new seeds hash key
    new_seeds = SyncSet()
    # remaining seeds hash key
    remaining_seeds = SyncSet()
    # seeds file
    seeds_file = ConfigService.get_seeds_file() + "_" + util.start_date
    # new_seeds_file
    new_seeds_file = ConfigService.get_new_seeds_file() + "_" + util.start_date

    _get_remaining_seeds_lock = Lock()

    logger = logging.getLogger('SeedsService')

    @classmethod
    def start(cls):
        # load the seeds template from the configuration file
        # template(seed_type, seed_target, source) = wanted_template
        instance = ConfigService.get_instance()
        seeds = instance["seeds_template"]
        for seed_type in seeds:
            for seed_with_name in seeds[seed_type]:
                for source in seed_with_name:
                    for seed_target in seed_with_name[source]:
                        seed_target_part = seed_with_name[source][seed_target]
                        seed_template_tmp = Seed()
                        for attr_name, attr_value in seed_target_part.items():
                            if attr_name == 'fields':
                                attr_value = parse_fields(attr_value)
                            setattr(seed_template_tmp, attr_name, attr_value)
                        seed_template_tmp.seed_type = seed_type
                        seed_template_tmp.seed_target = seed_target
                        seed_template_tmp.source = source
                        cls.template[(seed_type, seed_target, source)] = seed_template_tmp
        # load the seeds defined in configuraion file
        seeds = instance['seeds_instance']
        for seed_type in seeds:
            for seed_with_name in seeds[seed_type]:
                for source in seed_with_name:
                    for seed_target in seed_with_name[source]:
                        seed_target_part = seed_with_name[source][seed_target]
                        urls = seed_target_part.get("url", [])
                        for url in urls:
                            seed_template = cls.get_template(seed_type, seed_target, source)
                            setattr(seed_template, 'url', url)
                            for attr_name, attr_value in seed_target_part.items():
                                if attr_name == "url":
                                    continue
                                setattr(seed_template, attr_name, attr_value)
                            seed_template.update_hash()
                            cls.config_seeds.put_nowait(seed_template)
                            cls.current_seeds.add(seed_template.hash_key)
        # load the seeds of last crawl
        with open(ConfigService.get_seeds_file()) as f:
            for line in f:
                tokens = line.split(',')
                hash_key = tokens[0]
                cls.remaining_seeds.add(hash_key)

    @classmethod
    def stop(cls):
        os.remove(ConfigService.get_seeds_file())
        os.rename(cls.seeds_file, ConfigService.get_seeds_file())
        os.remove(ConfigService.get_new_seeds_file())
        os.rename(cls.new_seeds_file, ConfigService.get_new_seeds_file())

    @classmethod
    def put(cls, seed):
        if not cls.current_seeds.exist(seed.hash_key):
            cls.new_seeds.add(seed.hash_key)
            cls.logger.info(f"Found new seed {str(seed)}")
            cls.current_seeds.add(seed.hash_key)
            cls.work_queue.put(seed)

    @classmethod
    def get(cls):
        # get seed from work queue first
        try:
            seed = cls.work_queue.get_nowait()
        except queue.Empty:
            # get seed from the configuration file first.
            try:
                seed = cls.config_seeds.get_nowait()
            except queue.Empty:
                if cls.remaining_seeds.empty():
                    seed = None
                else:
                    with cls._get_remaining_seeds_lock:
                        cls._get_remaining_seeds()
                        seed = cls.work_queue.get_nowait()

        if seed and cls.remaining_seeds.exist(seed.hash_key):
            cls.remaining_seeds.remove(seed.hash_key)
        return seed

    @classmethod
    def _get_remaining_seeds(cls):
        # load the seeds of last crawl
        with open(ConfigService.get_seeds_file()) as f:
            for line in f:
                tokens = line.split(',')
                hash_key = tokens[0]
                if cls.remaining_seeds.exist(hash_key):
                    seed_type = tokens[1]
                    seed_target = tokens[2]
                    source = tokens[3]
                    url = tokens[4]
                    seed_template = cls.get_template(seed_type, seed_target, source)
                    setattr(seed_template, 'url', url)
                    cls.work_queue.put(seed_template)
                    cls.remaining_seeds.remove(hash_key)

    @classmethod
    def get_template(cls, seed_type, seed_target, source):
        return deepcopy(cls.template[(seed_type, seed_target, source)])