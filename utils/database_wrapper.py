#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: database_wrapper.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-10-27 17:25:23

Description: 与数据库进行交互
"""

import json
import logging
import time

import psycopg2
import toml
# 在so文件中实现，因此定位不到，但可用
from psycopg2.errors import (DuplicateSchema, DuplicateTable, InterfaceError,
                             InvalidSchemaName, OperationalError,
                             UndefinedColumn, UndefinedTable)

try:
    # 不要使用DBUtils.PooledPg.PooledPg
    from DBUtils.PooledDB import PooledDB  # DBUtils.__version__ < 2.0
except (ModuleNotFoundError, Exception):
    # 不要使用dbutils.pooled_pg.PooledPg
    from dbutils.pooled_db import PooledDB  # dbutils.__version__ >= 2.0

logger = logging.getLogger('DataWizard.utils.database_wrapper')


class PostgresqlWrapper(object):
    """PostgreSQL的包装器

    功能包括：
        - 创建模式      (CREATE SCHEMA)
        - 创建普通表    (CREATE TABLE)
        - 动态创建超表  (CREATE Hypertable)
        - 动态添加列    (ADD COLUMN)
        - 插入数据      (INSERT data)
        - 查询数据      (SELECT data)
    """
    def __init__(self, conf):
        """初始化方法

        1. 初始化配置信息
        2. 创建与PostgreSQL的连接（连接池）
        3. 预先创建Schema和Hypertable

        :conf: 配置参数

        """
        # Database连接参数配置
        self._host = conf.get('host', '127.0.0.1')
        self._port = conf.get('port', 5432)
        self._user = conf.get('user', None)
        self._password = conf.get('password', None)
        self._dbname = conf.get('dbname', None)

        # Database.Pool配置
        pool_conf = conf.get('pool', dict())
        self._mincached = pool_conf.get('mincached', 10)
        self._maxcached = pool_conf.get('maxcached', 0)
        self._maxshared = pool_conf.get('maxshared', 0)
        self._maxconnections = pool_conf.get('maxconnections', 0)
        self._blocking = pool_conf.get('blocking', True)
        self._maxusage = pool_conf.get('maxusage', 0)
        self._ping = pool_conf.get('ping', 1)

        # Database.Table配置
        column_conf = conf.get('column', dict())
        self._column_ts = column_conf.get('column_ts', 'timestamp')
        self._column_id = column_conf.get('column_id', 'deviceid')

        # message数据配置
        message_conf = conf.get('message', dict())
        self._message_switch = message_conf.get('message_switch', False)
        self._message_schema = message_conf.get('message_schema', 'public')
        self._message_table = message_conf.get('message_table', 'message')
        self._message_column = message_conf.get('message_column', list())

        # 创建PostgreSQL连接对象
        self._database = None
        self.connect()

    def _create_pool(self):
        """创建PostgreSQL连接池

        用于DBUtils连接池的参数有：
            - mincached           # 初始空闲连接数
            - maxcached           # 最大空闲连接数
            - maxconnections      # 允许的最大连接数
            - blocking            # 是否阻塞直到有空闲连接
            - maxusage            # 单个连接是否无限重用
            - ping                # 创建cursor时检测连接

        :returns: 连接池对象
        """
        pool = PooledDB(
            # DBUtils参数
            creator=psycopg2,
            mincached=self._mincached,
            maxcached=self._maxcached,
            maxshared=self._maxshared,
            maxconnections=self._maxconnections,
            blocking=self._blocking,
            maxusage=self._maxusage,
            ping=self._ping,
            # psycopg2参数
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            dbname=self._dbname)

        return pool

    def _reconnect(self):
        """重开与PostgreSQL的连接"""
        if not self._database._closed:
            self._database.close()
        self.connect()

    def connect(self):
        """从连接池中获取一个PostgreSQL连接对象

        :returns: PostgreSQL连接对象

        """
        while True:
            try:
                pool_obj = self._create_pool()
                self._database = pool_obj.connection()
                logger.info('Persistent database is connected')
                break
            except OperationalError as err:
                logger.error(
                    'Persistent database connection error: {text}'.format(
                        text=err))
            except AttributeError:
                logger.error('Persistent database connection pool error, '
                             'check configuration file')
            except Exception as err:
                logger.error(err)

            time.sleep(2)

    def create_schema(self, schema):
        """创建Schema

        :schema: 要创建的Schema名

        """
        # 构建SQL语句
        SQL = "CREATE SCHEMA {schema};".format(schema=schema)

        # 执行SQL语句
        try:
            cursor = self._database.cursor()
            cursor.execute(SQL)
            self._database.commit()
        except DuplicateSchema as warn:
            logger.warning('Duplicate schema: {warn}'.format(warn=warn))
        except (OperationalError, InterfaceError):
            logger.error('Reconnect to the PostgreSQL...')
            self._reconnect()
        except Exception as err:
            logger.error(err)

    def create_table(self, schema, table, columns):
        """创建Table

        非超表（时序表）

        :schema: 使用的Schema名
        :table: 要创建的Table名
        :columns: Column名及其数据类型
               columns = {
                             'column1': 'int',
                             'column2': 'float',
                             'column3': 'str',
                             ... ...
                         }

        """
        # 构建SQL语句
        columns_name = "id SERIAL PRIMARY KEY"
        for column, type_ in columns.items():
            if type_ in ['int', 'float']:
                # int和float类型的数据默认存储为DOUBLE PRECISION
                data_type = 'DOUBLE PRECISION'
            elif type_ in ['json']:
                # json(list, dict)类型的数据默认存储为JSON
                data_type = 'JSON'
            else:
                # 其他类型的数据默认存储为VARCHAR
                data_type = 'VARCHAR'

            columns_name = ("{curr_columns}, "
                            "{new_columns} {attr_1} {attr_2}".format(
                                curr_columns=columns_name,
                                new_columns=column,
                                attr_1=data_type,
                                attr_2='NULL'))
        SQL = "CREATE TABLE {schema_name}.{table_name} ({columns});".format(
            schema_name=schema, table_name=table, columns=columns_name)

        # 执行SQL语句
        try:
            cursor = self._database.cursor()
            cursor.execute(SQL)
            self._database.commit()
        except DuplicateTable as warn:
            logger.warning('Create table: {text}'.format(text=warn))
        except (OperationalError, InterfaceError):
            logger.error('Reconnect to the PostgreSQL...')
            self._reconnect()
        except Exception as err:
            logger.error(err)

    def create_hypertable(self, schema, hypertable, columns):
        """创建Hypertable

        :schema: 使用的Schema名
        :hypertable: 要创建的Hypertable名
        :columns: Column名及其数据类型
                  columns = {
                                'column1': 'int',
                                'column2': 'float',
                                'column3': 'str',
                                'column4': 'json',
                                ... ...
                            }

        """
        # 构建SQL语句元素
        proc = 'create_hypertable'
        columns_name = ("{column_ts} TIMESTAMP NOT NULL, "
                        "{column_id} VARCHAR NOT NULL".format(
                            column_ts=self._column_ts,
                            column_id=self._column_id))
        for column, type_ in columns.items():
            if type_ in ['int', 'float']:
                # int和float类型的数据默认存储为DOUBLE PRECISION
                data_type = 'DOUBLE PRECISION'
            elif type_ in ['json']:
                # json(list, dict)类型的数据默认存储为JSON
                data_type = 'JSON'
            else:
                # 其他类型的数据默认存储为VARCHAR
                data_type = 'VARCHAR'

            columns_name = ("{curr_columns}, "
                            "{new_columns} {attr_1} {attr_2}".format(
                                curr_columns=columns_name,
                                new_columns=column,
                                attr_1=data_type,
                                attr_2='NULL'))

        SQL = "CREATE TABLE {schema_name}.{table_name} ({columns});".format(
            schema_name=schema, table_name=hypertable, columns=columns_name)
        SQL_JUDGE = ("SELECT EXISTS ("
                     "SELECT * "
                     "FROM pg_catalog.pg_proc "
                     "JOIN pg_namespace "
                     "ON pg_catalog.pg_proc.pronamespace = pg_namespace.oid "
                     "WHERE proname = 'create_hypertable' "
                     "AND pg_namespace.nspname = '{schema_name}');".format(
                         schema_name=schema))

        # 执行SQL语句
        try:
            # 获取cursor
            cursor = self._database.cursor()

            # 判断指定schema中是否存在create_hypertable存储过程
            cursor.execute(SQL_JUDGE)
            exist = cursor.fetchall()[0][0]
            proc_schema = schema
            if not exist:
                proc_schema = 'public'
                logger.warning("Stored procedure {schema_name}.{proc_name} "
                               "does not exist, "
                               "use public.{proc_name}".format(
                                   schema_name=schema, proc_name=proc))

            # 如果指定schema中没有create_hypertable存储过程，则使用public中的
            SQL_HYPERTABLE = ("SELECT {proc_schema_name}.{proc_name}("
                              "'{schema_name}.{table_name}', "
                              "'{column_ts}');".format(
                                  proc_schema_name=proc_schema,
                                  proc_name=proc,
                                  schema_name=schema,
                                  table_name=hypertable,
                                  column_ts=self._column_ts))

            cursor.execute(SQL)
            cursor.execute(SQL_HYPERTABLE)
            self._database.commit()  # 在建表并设置为超表之后统一commit,否则可能会建一个普通表
        except InvalidSchemaName as warn:  # Schema不存在
            # 尝试创建Schema
            logger.error('Undefined schema: {text}'.format(text=warn))
            logger.info('Creating schema...')
            self.create_schema(schema=schema)
        except DuplicateTable as warn:  # Hypertable已存在
            logger.warning('Duplicate hypertable: {text}'.format(text=warn))
        except (OperationalError, InterfaceError):
            logger.error('Reconnect to the PostgreSQL...')
            self._reconnect()
        except Exception as err:
            logger.error(err)

    def add_column(self, schema, table, columns):
        """添加Column

        :schema: 使用的Schema名
        :table: 使用的Table名
        :columns: Column名及其数据类型
                  columns = {
                                'column1': 'int',
                                'column2': 'float',
                                'column3': 'str',
                                'column4': 'json',
                                ... ...
                            }

        """
        try:
            cursor = self._database.cursor()
            # PostgreSQL限制了一次只能新增一列
            for column, type_ in columns.items():
                if type_ in ['int', 'float']:
                    # int和float类型的数据默认存储为DOUBLE PRECISION
                    data_type = 'DOUBLE PRECISION'
                elif type_ in ['json']:
                    # json(list, dict)类型的数据默认存储为JSON
                    data_type = 'JSON'
                else:
                    # 其他类型的数据默认存储为VARCHAR
                    data_type = 'VARCHAR'

                SQL = ("ALTER TABLE {schema_name}.{table_name} "
                       "ADD COLUMN IF NOT EXISTS {column_name} {data_type};".
                       format(schema_name=schema,
                              table_name=table,
                              column_name=column,
                              data_type=data_type))

                # 执行SQL语句
                cursor.execute(SQL)
                self._database.commit()
        except (OperationalError, InterfaceError):
            logger.error('Reconnect to the PostgreSQL...')
            self._reconnect()
        except Exception as err:
            logger.error(err)

    def fork_message(self, datas):
        """转储message数据到一个独立的数据表

        :datas: 包含message的数据，dict类型
        :returns: SQL语句

                datas = {
                    'timestamp': '2020-10-21 10:19:11',
                    'id': 'groot',
                    'fields': {
                        'message': {
                            'name': 'message',
                            'title': '日志信息',
                            'value': 'XXX在XXXX年XX月XX日XX时XX分XX秒停机',
                            'type': 'str',
                            'unit': None
                        },
                        'source': {
                            'name': 'source',
                            'title': '日志来源',
                            'value': 'TCP',
                            'type': 'str',
                            'unit': None
                        },
                        'level': {
                            'name': 'level',
                            'title': '日志等级',
                            'value': 3,
                            'type': 'int',
                            'unit': None
                        },
                        'logpath': {
                            'name': 'logpath',
                            'title': '日志路径',
                            'value': '/Path/to/logfile',
                            'type': 'str',
                            'unit': None
                        },
                    }
                }

        """
        # COLUMN NAME和COLUMN VALUE
        columns_name = str()  # COLUMN NAME field
        column_value = list()  # 单个COLUMN VALUE field
        columns_value = list()  # 多个COLUMN VALUE field

        # timestamp/id value
        timestamp = "{ts_field}".format(ts_field=datas.get('timestamp'))
        id_ = "{id_name}".format(id_name=datas.get('deviceid'))

        # 构建COLUMN NAME、COLUMN VALUE和COLUMN MARK
        # # 构建COLUMN NAME（固有列）
        columns_name = ",".join([self._column_ts, self._column_id])
        # # 构建COLUMN VALUE
        column_value.append(timestamp)  # 固有的时间戳列
        column_value.append(id_)  # 固有的ID列
        # # 构建COLUMN MARK
        columns_value_mark = ",".join(["%s", "%s"])  # 固有的MARK（时间戳和ID）
        # # 完善COLUMN NAME、COLUMN VALUE和COLUMN MARK
        for column in self._message_column:
            if column in datas.get('fields', dict()).keys():
                # 完善COLUMN NAME
                columns_name = ",".join([columns_name, column])
                # 完善COLUMN VALUE
                column_value.append(
                    datas.get('fields',
                              dict()).get(column, dict()).get('value', str()))
                # 完善COLUMN MARK
                columns_value_mark = ",".join([columns_value_mark, "%s"])

        # 构建插入值
        columns_value.append(column_value)

        # 构建SQL语句
        SQL = (
            "INSERT INTO {schema_name}.{table_name} ({column_name}) "
            "VALUES ({column_value});".format(
                # SCHEMA.TABLE
                schema_name=self._message_schema,
                table_name=self._message_table,
                # COLUMN NAME
                column_name=columns_name,
                # COLUMN VALUE
                column_value=columns_value_mark))

        return SQL, columns_value

    def insert_oldgen(self, datas):
        """向数据表批量插入数据
        参数datas类型是list时需要保证其中每个dict的'schema'.'table'一致，且每个dict的'fields'的key相同

        解析参数datas，构建列名字符串columns_name和每列的值column_value，
        然后将column_value组合成一个大列表columns_value，最后构建SQL语句进行批量插入

        :datas: 要插入的数据，可以是元素为dict的list或者单独的dict
                datas = [{
                    'timestamp': '2020-10-21 10:19:11',
                    'schema': 'alien',
                    'table': 'tree',
                    'id': 'groot',
                    'fields': {
                        'x': {
                            'name': 'x',
                            'title': 'X轴',
                            'value': 65.7,
                            'type': 'float',
                            'unit': 'mm'
                        },
                        ... ...
                    }
                }, {
                    'timestamp': '2020-10-21 10:19:21',
                    'schema': 'alien',
                    'table': 'tree',
                    'id': 'groot',
                    'fields': {
                        'x': {
                            'name': 'x',
                            'title': 'X轴',
                            'value': 43.1,
                            'type': 'float',
                            'unit': 'mm'
                        },
                        ... ...
                    }
                }]

        """
        tag = 0  # 运行进度标志
        # SQL_MSG、COLUMN NAME和COLUMN VALUE
        SQL_MSG = str()  # Message SQL statement
        schema = str()  # SCHEMA name
        table = str()  # TABLE name
        columns_name = str()  # COLUMN NAME field
        column_value = list()  # 单个COLUMN VALUE field
        columns_value = list()  # 多个COLUMN VALUE field
        columns_value_mark = str()  # 固有COLUMN的MARK（时间戳和ID）
        msgs_columns_value = list()  # 多个日志信息的COLUMN VALUE field

        if isinstance(datas, list):
            # schema.table value和timestamp/id value
            schema = "{schema_name}".format(
                schema_name=datas[0].get('schema', 'public'))
            table = "{table_name}".format(
                table_name=datas[0].get('table', 'example'))
            timestamp = "{ts_field}".format(
                ts_field=datas[0].get('timestamp', '0000-00-00 08:00:00'))
            id_ = "{id_name}".format(id_name=datas[0].get('deviceid', 'ID'))

            # 构建COLUMN NAME、COLUMN VALUE和COLUMN MARK
            # # 构建COLUMN NAME（固有列）
            columns_name = ",".join([self._column_ts, self._column_id])
            # # 构建COLUMN VALUE
            column_value.append(timestamp)  # 固有的时间戳列
            column_value.append(id_)  # 固有的ID列
            # # 构建COLUMN MARK
            columns_value_mark = ",".join(["%s", "%s"])  # 固有的MARK（时间戳和ID）
            # # 完善COLUMN NAME和COLUMN MARK
            for column, data in datas[0].get('fields', dict()).items():
                # 完善COLUMN NAME
                columns_name = ",".join([columns_name, column])
                # 完善COLUMN MARK
                columns_value_mark = ",".join([columns_value_mark, "%s"])
            # 完善COLUMN VALUE
            for data in datas:
                # 检索处理message数据，如果允许其转储且'message'是data.get('fields')的key
                if self._message_switch and 'message' in data.get('fields', dict()).keys():
                    SQL_MSG, msgs_columns_value = self.fork_message(datas=data)
                for column in data.get('fields', dict()).values():
                    # 构建COLUMN VALUE
                    column_value.append(column.get('value', None))
                # 合并多个VALUE
                columns_value.append(column_value)
                # 初始化column_value，防止两个data的value混淆
                column_value = list()  # 单个COLUMN VALUE field
                column_value.append(timestamp)  # 固有的时间戳列
                column_value.append(id_)  # 固有的ID列
        elif isinstance(datas, dict):
            # schema.table value和timestamp/id value
            schema = "{schema_name}".format(
                schema_name=datas.get('schema', 'public'))
            table = "{table_name}".format(
                table_name=datas.get('table', 'example'))
            timestamp = "{ts_field}".format(
                ts_field=datas.get('timestamp', '0000-00-00 08:00:00'))
            id_ = "{id_name}".format(id_name=datas.get('deviceid', 'ID'))

            # 构建COLUMN NAME、COLUMN VALUE和COLUMN MARK
            # # 构建COLUMN NAME（固有列）
            columns_name = ",".join([self._column_ts, self._column_id])
            # # 构建COLUMN VALUE
            column_value.append(timestamp)  # 固有的时间戳列
            column_value.append(id_)  # 固有的ID列
            # # 构建COLUMN MARK
            columns_value_mark = ",".join(["%s", "%s"])  # 固有的MARK（时间戳和ID）
            # # 完善COLUMN NAME、COLUMN VALUE和COLUMN MARK
            for column, data in datas.get('fields', dict()).items():
                # 完善COLUMN NAME
                columns_name = ",".join([columns_name, column])
                # 完善COLUMN VALUE
                if data.get('type', 'str') == 'json':
                    value = json.dumps(data.get('value', None))
                else:
                    value = data.get('value', None)
                column_value.append(value)
                # 完善COLUMN MARK
                columns_value_mark = ",".join([columns_value_mark, "%s"])
            # 合并多个COLUMN VALUE
            columns_value.append(column_value)

            # 检索处理message数据，如果允许其转储且'message'是data.get('fields')的key
            if self._message_switch and 'message' in datas.get('fields', dict()).keys():
                SQL_MSG, msgs_columns_value = self.fork_message(datas=datas)
        else:
            logger.error("Data type error, 'datas' must be list or dict")

        # 构建SQL语句
        SQL = (
            "INSERT INTO {schema_name}.{table_name} ({column_name}) "
            "VALUES ({column_value});".format(
                # SCHEMA.TABLE
                schema_name=schema,
                table_name=table,
                # COLUMN NAME
                column_name=columns_name,
                # COLUMN VALUE
                column_value=columns_value_mark))

        # 执行SQL语句
        try:
            cursor = self._database.cursor()

            tag = 0
            cursor.executemany(SQL, columns_value)
            self._database.commit()
            logger.info(
                'Data inserted into ({schema_name}.{table_name}) successfully'.
                format(schema_name=schema, table_name=table))

            tag = 1
            if SQL_MSG:
                cursor.executemany(SQL_MSG, msgs_columns_value)
                self._database.commit()
                logger.info('Data inserted into '
                            '({schema_name}.{table_name}) '
                            'successfully'.format(
                                schema_name=self._message_schema,
                                table_name=self._message_table))
        except UndefinedTable as warn:
            # 数据库中缺少指定Table，动态创建
            logger.error('Undefined table: {text}'.format(text=warn))
            # 尝试创建Schema
            logger.info('Creating schema...')
            # # 根据tag（指明了try中运行到哪一步）决定参数值
            curr_schema = schema if tag == 0 else self._message_schema
            curr_table = table if tag == 0 else self._message_table
            self.create_schema(schema=curr_schema)
            # 尝试创建Hypertable
            logger.info('Creating hypertable...')
            columns = dict()
            # # 根据datas的类型取到它的'fields'
            cache = datas if isinstance(datas, dict) else datas[0]
            # # 根据tag（指明了try中运行到哪一步）决定参数值
            for key, value in cache.get('fields', dict()).items():
                if tag == 1:
                    if key in self._message_column:
                        columns.update({key: value.get('type', 'str')})
                else:
                    columns.update({key: value.get('type', 'str')})
            self.create_hypertable(schema=curr_schema,
                                   hypertable=curr_table,
                                   columns=columns)
            # 尝试再次写入数据
            cursor = self._database.cursor()
            cursor.executemany(SQL, columns_value)
            self._database.commit()
            logger.info(
                'Data inserted into ({schema_name}.{table_name}) successfully'.
                format(schema_name=curr_schema, table_name=curr_table))
            if SQL_MSG:
                cursor.executemany(SQL_MSG, msgs_columns_value)
                self._database.commit()
                logger.info('Data inserted into '
                            '({schema_name}.{table_name}) '
                            'successfully'.format(schema_name=curr_schema,
                                                  table_name=curr_table))
        except UndefinedColumn as warn:
            # 数据表中缺少指定Column，动态创建
            logger.warning('Undefined column: {text}'.format(text=warn))
            # 尝试添加Column
            logger.info('Adding column...')
            # # 根据tag（指明了try中运行到哪一步）决定参数值
            curr_schema = schema if tag == 0 else self._message_schema
            curr_table = table if tag == 0 else self._message_table
            # # 根据datas的类型取到它的'fields'
            cache = datas if isinstance(datas, dict) else datas[0]
            columns = dict()
            # # 根据tag（指明了try中运行到哪一步）决定参数值
            for key, value in cache.get('fields', dict()).items():
                if tag == 1:
                    if key in self._message_column:
                        columns.update({key: value.get('type', 'str')})
                else:
                    columns.update({key: value.get('type', 'str')})
            self.add_column(schema=curr_schema,
                            table=curr_table,
                            columns=columns)
            # 尝试再次写入数据
            cursor = self._database.cursor()
            cursor.executemany(SQL, columns_value)
            self._database.commit()
            logger.info(
                'Data inserted into ({schema_name}.{table_name}) successfully'.
                format(schema_name=curr_schema, table_name=curr_table))
            if SQL_MSG:
                cursor.executemany(SQL_MSG, msgs_columns_value)
                self._database.commit()
                logger.info('Data inserted into '
                            '({schema_name}.{table_name}) '
                            'successfully'.format(schema_name=curr_schema,
                                                  table_name=curr_table))
        except (OperationalError, InterfaceError):
            logger.error('Reconnect to the PostgreSQL...')
            self._reconnect()
        except Exception as err:
            logger.error(err)

    def insert(self, material):
        """向数据表批量插入数据

        :material: 一个字典，数据入库用到的物料

        """
        schema = material.get('schema', 'public')
        table = material.get('table', 'example')
        sql = material.get('sql', None)
        value = material.get('value', None)
        column_type = material.get('column', dict())

        try:
            # 执行SQL语句
            cursor = self._database.cursor()
            cursor.executemany(sql, value)
            self._database.commit()
            logger.info(
                'Data inserted into ({schema_name}.{table_name}) successfully'.
                format(schema_name=schema, table_name=table))
        except UndefinedTable as e:
            # 数据库中缺少指定Table，动态创建
            logger.error('Undefined table: {text}'.format(text=e))
            logger.info('Creating schema...')
            self.create_schema(schema=schema)
            logger.info('Creating hypertable...')
            self.create_hypertable(schema=schema,
                                   hypertable=table,
                                   columns=column_type)

            # 尝试再次执行SQL语句
            cursor = self._database.cursor()
            cursor.executemany(sql, value)
            self._database.commit()
            logger.info(
                'Data inserted into ({schema_name}.{table_name}) successfully'.
                format(schema_name=schema, table_name=table))
        except UndefinedColumn as e:
            # 数据表中缺少指定Column，动态创建
            logger.warning('Undefined column: {text}'.format(text=e))
            logger.info('Adding column...')
            self.add_column(schema=schema, table=table, columns=column_type)

            # 尝试再次执行SQL语句
            cursor = self._database.cursor()
            cursor.executemany(sql, value)
            self._database.commit()
            logger.info(
                'Data inserted into ({schema_name}.{table_name}) successfully'.
                format(schema_name=schema, table_name=table))
        except (OperationalError, InterfaceError):
            # 与数据库的连接断开，重新连接
            logger.error('Reconnect to the PostgreSQL...')
            self._reconnect()
        except Exception as e:
            # 未知错误
            logger.error(e)

    def query(self, schema, table, column='*', order='id', limit=5):
        """从指定的表查询指定数据

        :schema: 查询的Schema
        :table: 查询的Table
        :column: 查询的Column，形如'timestamp,id,x'
        :order: 以order排序
        :limit: 限制查询数量为limit
        :return: 查询结果，是个由元组组成的的列表

        """
        # 返回的查询结果
        result = list()

        # 构建SQL语句
        SQL = ("SELECT {column} FROM {schema_name}.{table_name} "
               "ORDER BY {order} DESC LIMIT {limit};".format(
                   column=column,
                   schema_name=schema,
                   table_name=table,
                   order=order,
                   limit=limit))

        # 执行SQL语句
        try:
            cursor = self._database.cursor()
            cursor.execute(SQL)
            result = cursor.fetchall()
            self._database.commit()
        except (UndefinedTable, UndefinedColumn) as warn:
            logger.error('Query error: {text}'.format(text=warn))
        except (OperationalError, InterfaceError):
            logger.error('Reconnect to the PostgreSQL...')
            self._reconnect()
        except Exception as err:
            logger.error(err)

        return result

    def use4test(self):
        """Use for test"""
        # 构建SQL语句
        SQL = "SHOW timezone;"

        # 执行SQL语句
        try:
            cursor = self._database.cursor()
            cursor.execute(SQL)
            data = cursor.fetchall()
            self._database.commit()
            print(data)
        except (OperationalError, InterfaceError):
            logger.error('Reconnect to the PostgreSQL...')
            self._reconnect()
        except Exception as err:
            logger.error(err)


if __name__ == "__main__":
    # 导入测试数据
    import sys
    sys.path.append('..')
    from plugins.parser_postgresql import parse_data
    from tools.genesis import genesis

    # 加载配置文件
    confile = '../conf/config.toml'
    config = toml.load(confile)

    # 创建与PostgreSQL的连接
    storage_conf = config.get('storage', dict())
    postgresql_conf = storage_conf.get('postgresql', dict())
    client = PostgresqlWrapper(postgresql_conf)

    # 运行简单测试方法
    while True:
        client.use4test()
        time.sleep(1)

        # 测试插入数据，一条数据有578列
        datas = genesis()
        result = parse_data(flow='postgresql',
                            config=storage_conf,
                            datas=datas)
        client.insert(material=result)
        print('>>> Data inserted')

        # 测试查询数据
        columns = 'timestamp,id'
        column_conf = postgresql_conf.get('column', dict())
        result = client.query(column=columns,
                              schema=datas.get('schema'),
                              table=datas.get('table'),
                              order=column_conf.get('column_ts'),
                              limit=1)
        print('Query result: \n{result}\n'.format(result=result))
