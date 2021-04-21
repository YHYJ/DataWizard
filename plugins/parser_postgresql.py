#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: parser_postgresql.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2021-04-08 18:48:04

Description: 为PostgreSQL进行原始数据解析
"""

import json
import logging

logger = logging.getLogger('DataWizard.plugins.parser_postgresql')


def parse_system_monitor(flow, config, datas):
    """解析系统监视信息
    根据datas解析出SQL语句及其需要的数据

    :flow: 数据流向，决定使用storage配置中的哪个部分
    :config: storage部分配置信息
    :datas: 要插入的数据，可以是元素为dict的list或者单独的dict
    :returns: SQL语句和已解析数据组成的字典，{'sql': 'SQL statement', 'data': 'Parsed data'}

    """
    # 定义变量
    schema = str()  # schema名
    table = str()  # table名
    columns_name = str()  # 所有列名组成的字符串
    column_value = list()  # 单一dict中的列值组成的列表
    columns_value = list()  # 多个column_value组成的列表
    column_value_mark = str()  # 单一dict中的列值的占位字符串

    # 如果数据流向PostgreSQL
    if flow.lower() in ['postgresql']:
        # 获取配置信息
        db_conf = config.get(flow, dict())
        fixed_columns = db_conf.get('column', dict())
        column_ts_tag = fixed_columns.get('column_ts', 'timestamp')
        column_id_tag = fixed_columns.get('column_id', 'id')

        if isinstance(datas, dict):
            # 获取schem.table名、非空列名和数据字段
            schema = datas.get('schema', 'public')
            table = datas.get('table', 'no_table')
            column_ts = datas.get(column_ts_tag, '1970-01-01 08:00:00')
            column_id = datas.get(column_id_tag, 'no_id')
            fields = datas.get('fields', dict())

            # 构建列名字符串 - 非空列
            columns_name = ','.join([column_ts_tag, column_id_tag])
            # 构建列值列表 - 非空列
            column_value.append(column_ts)
            column_value.append(column_id)
            # 构建列值占位字符串 - 非空列
            column_value_mark = ','.join(['%s'] * len(fixed_columns))

            # 补充列名字符串、列值列表和列值占位字符串 - 其他列
            for name, data in fields.items():
                columns_name = ','.join([columns_name, name])
                if data.get('type', None) == 'json':
                    value = json.dumps(data.get('value', None))
                else:
                    value = data.get('value', None)
                column_value.append(value)
                column_value_mark = ','.join([column_value_mark, '%s'])
            # 合并列值列表成一个大列表
            columns_value.append(column_value)
        elif isinstance(datas, list):
            # 获取第一个dict的schem.table名、非空列名和数据字段
            schema = datas[0].get('schema', 'public')
            table = datas[0].get('table', 'no_table')
            column_ts = datas[0].get(column_ts_tag, '1970-01-01 08:00:00')
            column_id = datas[0].get(column_id_tag, 'no_id')
            fields = datas[0].get('fields', dict())

            # 构建列名字符串 - 非空列
            columns_name = ','.join([column_ts_tag, column_id_tag])
            # 构建列值占位字符串 - 非空列
            column_value_mark = ','.join(['%s'] * len(fixed_columns))

            # 补充列名字符串和列值占位字符串 - 其他列
            for name, data in fields.items():
                columns_name = ','.join([columns_name, name])
                column_value_mark = ','.join([column_value_mark, '%s'])

            # 构建列值列表
            for data in datas:
                # 初始化列值列表
                column_value = list()
                # 补充列值列表 - 非空列
                column_value.append(column_ts)
                column_value.append(column_id)
                # 补充列值列表 - 其他列
                for field in data.get('fields', dict()).values():
                    if field.get('type', None) == 'json':
                        value = json.dumps(field.get('value', None))
                    else:
                        value = field.get('value', None)
                    column_value.append(value)
                # 合并列值列表成一个大列表
                columns_value.append(column_value)
        else:
            logger.error('Datas type error, must be dict or list')

        # 构建SQL语句
        SQL = ("INSERT INTO {schema_name}.{table_name} ({column_name}) "
               "VALUES ({column_value});").format(
                   schema_name=schema,
                   table_name=table,
                   column_name=columns_name,
                   column_value=column_value_mark)

        # 构建返回值
        result = dict()
        result['schema'] = schema
        result['table'] = table
        result['sql'] = SQL
        result['data'] = columns_value

        return result
