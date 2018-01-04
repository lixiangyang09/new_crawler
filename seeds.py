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
import shutil


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
                 hash_code=""):
        self.seed_type = seed_type_source
        self.seed_target = seed_type_target
        self.url = url
        self.source = source
        self.loadJS = loadJS
        self.validate = validate
        self.fields = fields
        self.hash_code = hash_code

    def update_hash(self):
        self.hash_code = util.get_hash(self.url)

    def __repr__(self):
        return self.hash_code + ',' + self.seed_type + ',' + self.seed_target + ',' + self.source + ',' + self.url


class SeedsService:
    # template(seed_source_type, seed_target_source, source) = wanted_template
    template = dict()
    # seeds(seed_source_type, seed_target_source)
    config_seeds = queue.Queue(0)
    # work queue
    work_queue = queue.Queue(0)
    # Seed processed until now
    current_seeds = SyncSet()
    # Seed of last time
    last_seeds = SyncSet()
    # remaining seeds hash key
    remaining_seeds = SyncSet()
    # seeds file
    seeds_file = ConfigService.get_seeds_file() + "_" + util.start_date
    seeds_file_handle = None
    # new_seeds_file
    new_seeds_file = ConfigService.get_new_seeds_file() + "_" + util.start_date

    _get_remaining_seeds_lock = Lock()

    logger = logging.getLogger('SeedsService')

    if os.path.exists(seeds_file):
        os.remove(seeds_file)
    if os.path.exists(new_seeds_file):
        os.remove(new_seeds_file)

    @classmethod
    def start(cls):
        # load the seeds template from the configuration file
        # template(seed_type, seed_target, source) = wanted_template
        cls.logger.info("Load seed template")
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
        cls.logger.info("Load seed instance from configuration file")
        configuration_seeds_count = 0
        seeds = instance['seeds_instance']
        for seed_type in seeds:
            for seed_with_name in seeds[seed_type]:
                for source in seed_with_name:
                    for seed_target in seed_with_name[source]:
                        seed_target_part = seed_with_name[source][seed_target]
                        urls = seed_target_part.get("url", [])
                        if urls is None:
                            urls = []
                        for url in urls:
                            seed_template = cls.get_template(seed_type, seed_target, source)
                            setattr(seed_template, 'url', url)
                            for attr_name, attr_value in seed_target_part.items():
                                if attr_name == "url":
                                    continue
                                setattr(seed_template, attr_name, attr_value)
                            seed_template.update_hash()
                            cls._save_seeds(seed_template)
                            cls.config_seeds.put_nowait(seed_template)
                            cls.current_seeds.add(seed_template.hash_code)
                            configuration_seeds_count += 1
        cls.logger.info(f"Total load {str(configuration_seeds_count)} seeds from configuration file")
        # load the seeds of last crawl
        cls.logger.info("Load seeds hash of last time crawled to remaining_seeds")
        if os.path.exists(ConfigService.get_seeds_file()):
            last_seeds_count = 0
            with open(ConfigService.get_seeds_file()) as f:
                for line in f:
                    line_tmp = line.strip()
                    if line_tmp:
                        tokens = line.split(',')
                        hash_code = tokens[0]
                        cls.remaining_seeds.add(hash_code)
                        cls.last_seeds.add(hash_code)
                        last_seeds_count += 1
            cls.seeds_file_handle = open(ConfigService.get_seeds_file())
            cls.logger.info(f"Total load {str(last_seeds_count)} seeds from {ConfigService.get_seeds_file()}")
        else:
            cls.logger.warning(f"{ConfigService.get_seeds_file()} not exists.")

    @classmethod
    def stop(cls):
        if os.path.exists(cls.seeds_file):
            if os.path.exists(ConfigService.get_seeds_file()):
                os.remove(ConfigService.get_seeds_file())
            # os.rename(cls.seeds_file, ConfigService.get_seeds_file())
            shutil.copy(cls.seeds_file, ConfigService.get_seeds_file())
        else:
            cls.logger.warning(f"{cls.seeds_file} not exists.")

        if os.path.exists(cls.new_seeds_file):
            if os.path.exists(ConfigService.get_new_seeds_file()):
                os.remove(ConfigService.get_new_seeds_file())
            # os.rename(cls.new_seeds_file, ConfigService.get_new_seeds_file())
            shutil.copy(cls.new_seeds_file, ConfigService.get_new_seeds_file())
        else:
            cls.logger.warning(f"{cls.new_seeds_file} not exists.")

        if cls.seeds_file_handle:
            cls.seeds_file_handle.close()

    @classmethod
    def put(cls, seed):
        cls._save_seeds(seed)
        if not cls.current_seeds.exist(seed.hash_code):
            cls.logger.info(f"Found new seed {str(seed)}")
            cls.current_seeds.add(seed.hash_code)
            cls.work_queue.put(seed)

    @classmethod
    def _save_seeds(cls, seed):
        """
        save seeds to local file or redis in the future
        :param seed:
        :return:
        """
        if not cls.current_seeds.exist(seed.hash_code):
            with open(cls.seeds_file, 'a') as f:
                f.write(str(seed) + "\n")

            if not cls.last_seeds.exist(seed.hash_code):
                with open(cls.new_seeds_file, 'a') as f:
                    f.write(str(seed) + "\n")

    @classmethod
    def get(cls):
        # get seed from work queue first
        try:
            seed = cls.work_queue.get_nowait()
        except queue.Empty:
            cls.logger.warning(f"Working queue is empty now, try to get config seed.")
            # get seed from the configuration file first.
            try:
                seed = cls.config_seeds.get_nowait()
                cls.logger.warning(f"Get seed from config queue")
            except queue.Empty:
                cls.logger.warning(f"Config queue is empty now, try to get remaining seed.")
                if cls.remaining_seeds.empty():
                    seed = None
                    cls.logger.warning(f"No remaining seed.")
                else:
                    with cls._get_remaining_seeds_lock:
                        seed = cls._get_remaining_seed()

        if seed and cls.remaining_seeds.exist(seed.hash_code):
            cls.remaining_seeds.remove(seed.hash_code)
        return seed

    @classmethod
    def _get_remaining_seed(cls):
        res_seed = None
        for line in cls.seeds_file_handle:
            tokens = line.split(',')
            hash_code = tokens[0]
            if cls.remaining_seeds.exist(hash_code) and not cls.current_seeds.exist(hash_code):
                seed_type = tokens[1]
                seed_target = tokens[2]
                source = tokens[3]
                url = tokens[4]
                seed_template = cls.get_template(seed_type, seed_target, source)
                seed_template.hash_code = hash_code
                seed_template.url = url
                cls.remaining_seeds.remove(hash_code)
                cls.current_seeds.add(hash_code)
                res_seed = seed_template
                break
        return res_seed

    @classmethod
    def get_template(cls, seed_type, seed_target, source):
        return deepcopy(cls.template[(seed_type, seed_target, source)])