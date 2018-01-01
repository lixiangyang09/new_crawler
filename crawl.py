#!/usr/bin/env python
# encoding=utf8

import time
import logging
import traceback
from threading import Lock, BoundedSemaphore
from concurrent.futures import ThreadPoolExecutor, wait
from seeds import SeedsService
from request import RequestService
from extract import ExtractService
from process import ProcessService


class RunContext:

    def __init__(self, initial_runners=0):
        self.runners = initial_runners
        self._lock = Lock()

    def __enter__(self):
        with self._lock:
            self.runners += 1

    def __exit__(self, exe_type, exec_val, tb):
        with self._lock:
            self.runners -= 1

    @property
    def is_running(self):
        return self.runners > 0


class CrawlService:
    logger = logging.getLogger(__name__)
    number_threads = 3
    semaphore = BoundedSemaphore(number_threads)
    run_context = RunContext()

    @classmethod
    def start(cls):
        cls.logger.info(f"Crawl service start to work with {str(cls.number_threads)} threads.")

        pool = ThreadPoolExecutor(cls.number_threads)
        futures = []

        while True:
            cls.semaphore.acquire()

            seed = SeedsService.get()
            if seed is None:
                cls.semaphore.release()
                if cls.run_context.is_running:
                    time.sleep(2)  # maybe the running thread will generate new seed
                    continue
                else:
                    break
            else:
                future = pool.submit(cls._crawl_framework, seed)
                futures.append(future)

        wait(futures)
        cls.logger.info("all work has been finished.")

    @classmethod
    def _crawl_framework(cls, seed):
        try:
            with cls.run_context:
                cls.logger.info(f"Start processing seed {str(seed)}")
                res = RequestService.request(seed)
                if res[0] == 200:
                    extract_status, data = ExtractService.extract(seed, res[2])
                    cls.logger.info(f"extract_status: {str(extract_status)}")
                    if extract_status:  # thought return ok, but the content is not wanted
                        ProcessService.process(seed, data)
                        cls.logger.info(f"Successfully processing seed {str(seed)}")
                else:
                    cls.logger.warning(f"Return code {str(res[0])} of seed {str(seed)}")
                time.sleep(3)
        except BaseException:
            cls.logger.error(traceback.format_exc())
        finally:
            cls.semaphore.release()

