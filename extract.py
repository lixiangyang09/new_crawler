#!/usr/bin/env python
# encoding=utf8


import queue
from lxml import html
import util
from store import FileService
import logging
import re
import urllib.parse

logger = logging.getLogger("ExtractService")


class ExtractService:
    url_pattern = re.compile(r'(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]')
    index_pattern = re.compile(r'(.*/)(\d+)(.*)')

    @classmethod
    def _check_content_expected(cls, content):
        """This function used to is_appeared whether the returned content is the normal data or validation"""
        res = True
        if not content or content is None or content == "block" or \
            content == "" or "未找到页面" in content \
                or "流量异常" in content:
            res = False
        return res

    @classmethod
    def extract(cls, seed, web_content):
        """
        extract data from crawled content
        :param seed:
        :param web_content:
        :return: status(Is successfully crawled?), data(the map based on the xpath)
        """
        if cls._check_content_expected(web_content):  # not the expected result, caused by proxy
            tree = html.fromstring(web_content)

            # validate the content
            if seed.validate is not None:
                validate_res = False
                for validate_xpath in seed.validate:
                    value = tree.xpath(validate_xpath)
                    if value:
                        validate_res = True
                        break
                if not validate_res:  # The page doesn't crawled successfully, return
                    return False, dict()

            data = dict()
            for target, field in seed.fields.items():
                content_xpaths = field.xpath
                field_value = ""
                if content_xpaths:
                    for content_xpath in content_xpaths:
                        if content_xpath.startswith("_"):
                            field_value = content_xpath[1:]
                        else:
                            field_value = tree.xpath(content_xpath)
                            if field.udf is not None and field_value:
                                field_value = cls._get_udf_result(seed, field, field_value)
                                if not field_value:  # after udf, the field should not be empty
                                    tmp_file = str(util.get_uuid())
                                    FileService.save_file("tmp", tmp_file, web_content)
                                    logger.warning(f"The filed value is empty after udf. The field: {field}, "
                                                   f"the seed: {seed}. "
                                                   f"Please check the file: {tmp_file} for more details.")
                                    return False, dict()

                            if field_value:  # status field may be empty
                                if field.name in ["ip", "port", "type", "page", "index"]:
                                    field_value = field_value
                                else:
                                    field_value = field_value[0].strip().replace('\xa0', '')
                        # keep the first nonempty xpath result
                        if field_value:
                            break
                else:
                    field_value = cls._get_udf_result(seed, field, field_value)

                if field.required and not field_value:
                    tmp_file = str(util.get_uuid())
                    FileService.save_file("tmp", tmp_file, web_content)
                    logger.warning(f"Can't get the field: {field}, when processing seed: {seed}."
                                   f"Please check the file: {tmp_file} for more details.")
                    return False, dict()
                data[field.name] = field_value
            data['source'] = seed.source
        cls.check_integrality(seed, data, web_content)
        return True, data

    @classmethod
    def check_integrality(cls, seed, data, web_content):
        if 'status' in data and data['status']:  # if status not empty, which means that the house is sold or off shelf.
            return
        fields_need_check = getattr(seed, 'normalfields', [])
        for field in fields_need_check:
            if field in data and data[field]:
                pass
            else:
                tmp_file = str(util.get_uuid())
                FileService.save_file("tmp", tmp_file, web_content)
                logger.warning(f"Can't get field {field} of seed {str(seed)}."
                               f"Please check the web_content file {tmp_file} for more details.")
                break

    @classmethod
    def join_url(cls, base, part):
        tmp_url = urllib.parse.urljoin(base, part)
        return cls.validate_url(tmp_url)

    @classmethod
    def validate_url(cls, data):
        # it could be the url or None
        validated = cls.url_pattern.match(data)
        if validated:
            return validated.group()
        else:
            return None

    @classmethod
    def _get_udf_result(cls, seed, field, field_value):
        result = []
        name = field.udf['func']
        if name == "lianjiaIndexUDF":
            for index in field_value:
                attribs = index.attrib
                comp_module = attribs.get("comp-module", None)
                page_url = attribs.get('page-url', None)
                page_data_string = attribs['page-data']
                if comp_module and page_url and page_data_string:
                    page_data = eval(page_data_string)
                    total_pages = page_data['totalPage']
                    cur_page = page_data['curPage']
                    for x in range(cur_page, total_pages + 1):
                        tmp_result = page_url.replace("{" + comp_module + "}", str(x))
                        tmp_result = cls.join_url(seed.url, tmp_result)
                        result.append(tmp_result)
        elif name == "pageListUDF":
            end_url = field.udf.get('end_url', None)
            # get the number list
            index_nums = []
            for index in field_value:
                match_res = cls.index_pattern.match(index)
                if match_res:
                    part_1 = match_res.group(1)
                    part_3 = match_res.group(3)
                    index_num = int(match_res.group(2))
                    index_nums.append(index_num)
            start = min(index_nums)
            end = max(index_nums)
            for num in range(start, end):
                res = cls.join_url(seed.url, part_1 + str(num) + part_3)
                if not end_url or (len(res) < len(end_url) or res < end_url):
                    result.append(res)
                else:
                    break
        elif name == 'shouldNotBe':
            should_not_be = field.udf['values']
            for need_check in field_value:
                res = True
                for not_be in should_not_be:
                    if need_check == not_be:
                        res = False
                        break
                if res:
                    result.append(need_check)
        elif name == 'getCity':
            url = urllib.parse.urlparse(seed.url)
            result = url.hostname.split('.')[0]
        return result
