#!/usr/bin/env python
# encoding=utf8

import requests
import re
import chardet
import logging
import random
from proxy import ProxyService
import traceback


class RequestService:
    logger = logging.getLogger(__name__)

    session = requests.session()
    charset_pattern = re.compile(r"(charset=')(.*)(')")

    @classmethod
    def request(cls, seed):
        url = seed.url
        load_js = seed.loadJS
        result = 404, url, ""
        if load_js:
            pass
        else:
            result = cls.normal_request(url)
        return result

    @classmethod
    def normal_request(cls, url):
        """
        Use proxy to request data from url
        :param url:
        :return: status code, the new url(maybe redirect), web_content
        """
        ua = cls.get_random_desktop_ua()
        cls.session.headers.update({'User-Agent': ua})
        request_timeout = 5
        data = ""
        while True:
            try:
                proxy_instance = ProxyService.get()
                cls.logger.info(f"In processing seed url {str(url)} with proxy: {str(proxy_instance)}")
                if proxy_instance:
                    proxy_data = proxy_instance.ip + ":" + proxy_instance.port
                    response = cls.session.get(url, proxies={'http': proxy_data}, timeout=request_timeout)
                else:
                    response = cls.session.get(url, timeout=request_timeout)

                status_code = response.status_code

                if response.status_code == requests.codes.ok:
                    response_content = response.content
                    guess_encoding = chardet.detect(response_content)
                    if guess_encoding:
                        content = response_content.decode(guess_encoding['encoding'])
                    else:
                        content = None
                    url = response.url
                    data = content

                # put the good proxy back into queue for next time use
                ProxyService.put(proxy_instance)
                break
            # current process doesn't, we need to change another process instance to crawl the same seed
            except requests.exceptions.RequestException:
                ProxyService.update_proxy(proxy_instance)
                # logger.warning(traceback.format_exc())
                # logger.warning(f"the seeds {seed.url} with the RequestException exception.")
                # traceback.print_exc()
            except BaseException as e:  # this may happen when the data is not expected
                # cls.logger.exception(e)
                status_code = 404
                cls.logger.info("Error processing seed: " + url + " with proxy: " + str(proxy_instance))
                # not realized exception, need to analysis
                # traceback.print_exc()
                cls.logger.warning(traceback.format_exc())
                break

        return status_code, url, data

    @classmethod
    def get_random_desktop_ua(cls):
        """generate a random user-agent of desktop browser"""
        platforms = ['Windows NT 6.1', 'Macintosh; Intel Mac OS X 10_10_1', 'X11; Linux x86_64',
                     'Windows NT 6.1; WOW64']
        products = [
            {
                'engines': ['AppleWebKit/537.36 (KHTML, like Gecko)'],  # chrome claims to based on webkit, not blink
                'name': 'Chrome',
                'version': ['58.0.3029.{}'.format(i) for i in range(100)],
                'base_product': 'Safari/537.36'
            },
            {
                'engines': ['Gecko/20100101'],  # gecko version number
                'name': 'Firefox',
                'version': ['{}.0'.format(i) for i in range(40, 60)],
                'base_product': ''
            }
        ]
        product = random.choice(products)
        return 'Mozilla/5.0 ({platform}) {engine} {name}/{version} {base_product}'.format(
            platform=random.choice(platforms),
            engine=random.choice(product.get('engines')),
            name=product.get('name'),
            version=random.choice(product.get('version')),
            base_product=product.get('base_product')
        )


if __name__ == '__main__':
    res = RequestService.normal_request('http://www.ip181.com/daili/1.html')
    print(res)
