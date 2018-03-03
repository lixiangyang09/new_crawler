#!/usr/bin/env python
# encoding=utf8

import hashlib
import uuid
from datetime import datetime, date
import os
from email.mime.base import MIMEBase
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import constants

start_date = str(date.today())
start_time = datetime.now()


def gen_hash(data):
    code = hashlib.md5(data.encode('utf-8')).hexdigest()
    return code


def get_uuid():
    return uuid.uuid1()


def get_output_base_dir():
    return 'output'


def get_output_data_dir():
    return get_output_base_dir() + "/" + start_date


def get_tmp_data_dir():
    return os.path.join(constants.tmp_data_dir, start_date)


def get_jianwei_data_dir():
    return os.path.join(get_output_base_dir(), constants.jianwei_data_dir)


def send_mail(from_address, passwd, to_addresses, subject, msg_body, attachments=[]):
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = ",".join(to_addresses)
    msg['Subject'] = subject
    msg.attach(MIMEText(msg_body, 'plain'))

    for file in attachments:
        with open(file, "rb") as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
        encoders.encode_base64(part)
        file_name = os.path.basename(file)
        part.add_header('Content-Disposition', 'attachment', filename=('gbk', '', file_name))
        msg.attach(part)

    # smtp = smtplib.SMTP()
    smtp = smtplib.SMTP_SSL("smtp.qq.com", 465)
    smtp.login(from_address, passwd)
    smtp.sendmail(from_address, to_addresses, msg.as_string())
    smtp.quit()


def get_line_hash(line):
    res = None
    line_tmp = line.strip()
    if line_tmp:
        tokens = line_tmp.split(',')
        if len(tokens) > 1:
            res = tokens[0]
    return res


def get_file_md5(file_full_path):
    md5file = open(file_full_path)
    md5 = hashlib.md5(md5file.read()).hexdigest()
    md5file.close()
    return md5
