#!/usr/bin/env python
# encoding=utf8


from store import FileService
import util
import os
import constants
import logging

logger = logging.getLogger(__name__)


def pack_report_data():
    report_data_dir = constants.report_data_dir
    output_base_dir = constants.output_base_dir
    if os.path.exists(report_data_dir):
        packed_files = [file_name for file_name in os.listdir(report_data_dir)
                        if file_name.endswith(constants.data_file_suffix)]
    else:
        packed_files = []
    packed_files_date = [os.path.basename(packed)[:os.path.basename(packed).rindex(constants.data_file_suffix)] for packed in packed_files]
    data_folders = [folder for folder in os.listdir(output_base_dir) if os.path.isdir(output_base_dir + '/' + folder)]
    for data_folder in data_folders:
        if data_folder not in packed_files_date:
            seeds_file = constants.seeds_file + '_' + data_folder
            if os.path.exists(seeds_file):
                logger.info(f"Ready to pack {data_folder}")
                pack_file_name = report_data_dir + '/' + data_folder + constants.data_file_suffix
                FileService.pack_files(pack_file_name, [(data_folder, output_base_dir),
                                                        (os.path.basename(seeds_file), os.path.dirname(seeds_file))])
                logger.info(f"Finish packing file {pack_file_name}")
            else:
                logger.info(f"Can't find {seeds_file}, failed to pack {data_folder}")
        else:
            logger.info(f"{data_folder} already exists in {report_data_dir}")