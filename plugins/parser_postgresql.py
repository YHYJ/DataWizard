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


def fork_message(conf, datas):
    """转储message数据到一个独立的数据表

    :datas: 包含message的数据，dict类型
    :returns: 字典，结构为：
              {
                  'schema': 'public',
                  'table': 'example',
                  'sql': 'SQL statement',
                  'value': 'Parsed data',
                  'column': {
                        'column_1': 'int',
                        'column_2': 'json',
                  }
              }

    """
    # 获取配置信息
    fixed_columns = conf.get('column', dict())
    column_ts_tag = fixed_columns.get('column_ts', 'timestamp')
    column_id_tag = fixed_columns.get('column_id', 'id')
    # message数据配置
    message_conf = conf.get('message', dict())
    message_schema = message_conf.get('message_schema', 'public')
    message_table = message_conf.get('message_table', 'message')
    message_column = message_conf.get('message_column', list())

    # 定义变量
    column_type = dict()  # 列名及其类型组成的字典
    columns_name = str()  # 所有列名组成的字符串
    column_value = list()  # 单一dict中的列值组成的列表
    columns_value = list()  # 多个column_value组成的列表
    column_value_mark = str()  # 单一dict中的列值的占位字符串

    column_ts = datas.get('timestamp', '1970-01-01 08:00:00')
    column_id = datas.get('deviceid', 'id')

    # 构建列名字符串 - 非空列
    columns_name = ','.join([column_ts_tag, column_id_tag])
    # 构建列值列表 - 非空列
    column_value.append(column_ts)
    column_value.append(column_id)
    # 构建列值占位字符串 - 非空列
    column_value_mark = ','.join(['%s'] * len(fixed_columns))

    # 补充列名字符串、列名类型字典、列值列表和列值占位字符串 - 其他列
    fields = datas.get('fields', dict())
    for name in message_column:
        if name in fields.keys():
            columns_name = ','.join([columns_name, name])
            column_type.update(
                {name, fields.get(name, dict()).get('type', 'str')})
            column_value.append(fields.get(name, dict()).get('value', str()))
            column_value_mark = ','.join([column_value_mark, '%s'])
    # 合并列值列表成一个大列表
    columns_value.append(column_value)

    # 构建SQL语句
    SQL = ("INSERT INTO {schema_name}.{table_name} ({column_name}) "
           "VALUES ({column_value});".format(schema_name=message_schema,
                                             table_name=message_table,
                                             column_name=columns_name,
                                             column_value=column_value_mark))

    # 构建返回值
    message = dict()
    message['schema'] = message_schema
    message['table'] = message_table
    message['sql'] = SQL
    message['value'] = columns_value
    message['column'] = column_type

    return message


def parse_data(flow, config, datas):
    """解析数据得到SQL语句
    根据datas解析出SQL语句及其需要的数据

    :flow: 数据流向，决定使用storage配置中的哪个部分
    :config: storage部分配置信息
    :datas: 要插入的数据，可以是元素为dict的list或者单独的dict
    :returns: 多个字典组成的列表，字典结构为：
              {
                  'schema': 'public',
                  'table': 'example',
                  'sql': 'SQL statement',
                  'value': 'Parsed data',
                  'column': {
                        'column_1': 'int',
                        'column_2': 'json',
                  }
              }

    """
    # 定义变量
    schema = str()  # schema名
    table = str()  # table名
    column_type = dict()  # 列名及其类型组成的字典
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
        # message数据配置
        message = dict()
        message_conf = db_conf.get('message', dict())
        message_switch = message_conf.get('message_switch', False)

        if isinstance(datas, dict):
            # 获取schem.table名、非空列名和数据字段
            schema = datas.get('schema', 'public')
            table = datas.get('table', 'example')
            column_ts = datas.get('timestamp', '1970-01-01 08:00:00')
            column_id = datas.get('deviceid', 'id')

            # 构建列名字符串 - 非空列
            columns_name = ','.join([column_ts_tag, column_id_tag])
            # 构建列值列表 - 非空列
            column_value.append(column_ts)
            column_value.append(column_id)
            # 构建列值占位字符串 - 非空列
            column_value_mark = ','.join(['%s'] * len(fixed_columns))

            # 补充列名字符串、列名类型字典、列值列表和列值占位字符串 - 其他列
            for name, data in datas.get('fields', dict()).items():
                columns_name = ','.join([columns_name, name])
                column_type.update({name: data.get('type', 'str')})
                if data.get('type', None) == 'json':
                    value = json.dumps(data.get('value', None))
                else:
                    value = data.get('value', None)
                column_value.append(value)
                column_value_mark = ','.join([column_value_mark, '%s'])
            # 合并列值列表成一个大列表
            columns_value.append(column_value)

            # 检索处理message数据
            if message_switch and 'message' in datas['fields'].keys():
                message = fork_message(conf=db_conf, datas=datas)
        elif isinstance(datas, list):
            # 获取第一个dict的schem.table名、非空列名和数据字段
            schema = datas[0].get('schema', 'public')
            table = datas[0].get('table', 'example')
            column_ts = datas[0].get('timestamp', '1970-01-01 08:00:00')
            column_id = datas[0].get('deviceid', 'id')

            # 构建列名字符串 - 非空列
            columns_name = ','.join([column_ts_tag, column_id_tag])
            # 构建列值占位字符串 - 非空列
            column_value_mark = ','.join(['%s'] * len(fixed_columns))

            # 补充列名字符串和列值占位字符串 - 其他列
            for name, data in datas[0].get('fields', dict()).items():
                columns_name = ','.join([columns_name, name])
                column_type.update({name: data.get('type', 'str')})
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

                # 检索处理message数据
                if message_switch and 'message' in data['fields'].keys():
                    message = fork_message(conf=db_conf, datas=data)
        else:
            logger.error("Data type error, 'datas' must be list or dict")

        # 构建SQL语句
        SQL = ("INSERT INTO {schema_name}.{table_name} ({column_name}) "
               "VALUES ({column_value});".format(
                   schema_name=schema,
                   table_name=table,
                   column_name=columns_name,
                   column_value=column_value_mark))

        # 构建返回值
        result = list()

        data = dict()
        data['schema'] = schema
        data['table'] = table
        data['sql'] = SQL
        data['value'] = columns_value
        data['column'] = column_type

        result.append(data)
        result.append(message)

        return result
