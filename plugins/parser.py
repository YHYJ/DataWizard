#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: parser.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-12-31 17:04:55

Description: 将数据解析为数据库需要的格式

使用方法：实现data_parser函数，接收原始数据和配置信息，返回目标数据
"""

import logging
import types

logger = logging.getLogger('DataWizard.plugins.parser')


def data_parser(raw_data, conf):
    """将原始数据(raw_data)解析为指定目标(storage)需要的SQL语句和数据组成的元组

    :conf: Configuration
    :raw_data: raw data, type is dict
    :returns: parse result

    """
    main_conf = conf.get('main', dict())
    storage = main_conf.get('data_storage', None)

    storage_conf = conf.get('storage', dict()).get(storage, dict())
    table_conf = storage_conf.get('table', dict())

    logger.info('Parse data for {}.'.format(storage))

    if storage == 'timescaledb':
        # 列名、每行的值及其占位符，用于构建和执行SQL语句
        columns_name = str()  # 所有列名
        value_row = list()  # 一行值
        value_rows = list()  # 多行值
        value_row_mark = str()  # 一行占位符
        if isinstance(raw_data, (list, types.GeneratorType)):
            # Schema name, Table name, timescale field, id field
            schema = '{}'.format(raw_data[0].get('schema', str()))
            table = '{}'.format(raw_data[0].get('table', str()))

            # 构建列名和行占位符
            columns_name = ','.join([column for column in table_conf.keys()])
            value_row_mark = ','.join(['%s' for _ in range(len(table_conf))])

            # 补全列名和行占位符
            for column, data in raw_data[0].get('fields', dict()).items():
                columns_name = ','.join([columns_name, column])
                value_row_mark = ','.join([value_row_mark, '%s'])

            # 构建并补全行值
            for data in raw_data:
                value_row = list()
                value_row.extend([
                    data.get(column_value, str())
                    for column_value in table_conf.values()
                ])
                for fields in data.get('fields', dict()).values():
                    value_row.append(fields.get('value', None))
                value_rows.append(value_row)
        elif isinstance(raw_data, (dict)):
            # Schema name, Table name, timescale field, id field
            schema = '{}'.format(raw_data.get('schema', str()))
            table = '{}'.format(raw_data.get('table', str()))

            # 构建列名、行值及行占位符
            columns_name = ','.join(
                [column_name for column_name in table_conf.keys()])
            value_row.extend([
                raw_data.get(column_value, str())
                for column_value in table_conf.values()
            ])
            value_row_mark = ','.join(['%s' for _ in range(len(table_conf))])

            # 补全列名和行占位符
            for column, fields in raw_data.get('fields', dict()).items():
                columns_name = ','.join([columns_name, column])
                value_row_mark = ','.join([value_row_mark, '%s'])
                value_row.append(fields.get('value', None))
            # 补全行值
            value_rows.append(value_row)
        else:
            logger.error('Data type error, must be list or dict.')

        # 构建SQL语句
        SQL = (
            "INSERT INTO {SCHEMA}.{TABLE} ({COLUMN_NAME}) "
            "VALUES ({VALUE_ROW_MARK});".format(
                # SCHEMA.TABLE
                SCHEMA=schema,
                TABLE=table,
                # COLUMN NAME
                COLUMN_NAME=columns_name,
                # VALUE ROW MARK
                VALUE_ROW_MARK=value_row_mark))

        # 构建返回值
        result = (SQL, value_rows)

        return result
    else:
        pass
