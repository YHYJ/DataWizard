#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: data_wrapper.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-12-31 17:04:55

Description: 将数据解析为数据库需要的格式
"""

import logging

logger = logging.getLogger(__name__)


def data_parser(raw_data, target):
    """将原始数据(raw_data)解析为指定目标(target)需要的数据

    :raw_data: raw data, type is dict
    :target: target, like 'timescale'
    :returns: parse result

    """
    if target == 'timescale':
        pass
