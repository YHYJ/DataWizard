#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: timescale_wrapper.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-10-27 17:25:23

Description: 与TimescaleDB进行数据交互，包括：
1. 创建模式(Schema)
2. 创建表(Table)
3. 创建超表(Hypertable)
4. 动态添加列(Column)
5. 批量插入数据
6. 查询数据

"""

import logging
import time
import types

import psycopg2
import toml
from psycopg2.errors import (DuplicateSchema, DuplicateTable, InterfaceError,
                             InvalidSchemaName, OperationalError,
                             UndefinedColumn, UndefinedTable)

try:
    # 不要使用DBUtils.PooledPg.PooledPg
    from DBUtils.PooledDB import PooledDB  # DBUtils.__version__ < 2.0
except (ModuleNotFoundError, Exception):
    # 不要使用dbutils.pooled_pg.PooledPg
    from dbutils.pooled_db import PooledDB  # dbutils.__version__ >= 2.0

log = logging.getLogger('TimescaleWrapper')


class TimescaleWrapper(object):
    """TimescaleDB的包装器

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
        2. 创建与TimescaleDB的连接（连接池）
        3. 预先创建Schema和Hypertable

        :conf: 配置参数

        """
        # Database连接参数配置
        self.host: str = conf.get('host', '127.0.0.1')
        self.port: int = conf.get('port', 5432)
        self.user: str = conf.get('user', None)
        self.password: str = conf.get('password', None)
        self.dbname: str = conf.get('dbname', None)

        # Database.Pool配置
        pool_conf: dict = conf.get('pool', dict())
        self.mincached: int = pool_conf.get('mincached', 10)
        self.maxcached: int = pool_conf.get('maxcached', 0)
        self.maxshared: int = pool_conf.get('maxshared', 0)
        self.maxconnections = pool_conf.get('maxconnections', 0)
        self.blocking: bool = pool_conf.get('blocking', True)
        self.maxusage: int = pool_conf.get('maxusage', 0)
        self.ping: int = pool_conf.get('ping', 1)

        # Database.Table配置
        table_conf: dict = conf.get('table', dict())
        self.column_time: str = table_conf.get('column_time', 'timestamp')
        self.column_id: str = table_conf.get('column_id', 'id')

        # 日志数据配置
        log_conf: dict = conf.get('log', dict())
        self.fork_log: bool = log_conf.get('fork_log', False)
        self.log_schema: str = log_conf.get('log_schema', 'monitor')
        self.log_table: str = log_conf.get('log_table', 'log')

        # 创建TimescaleDB连接对象
        self.database = None
        self.connect()

    def _createPool(self):
        """创建TimescaleDB连接池

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
            mincached=self.mincached,
            maxcached=self.maxcached,
            maxshared=self.maxshared,
            maxconnections=self.maxconnections,
            blocking=self.blocking,
            maxusage=self.maxusage,
            ping=self.ping,
            # psycopg2参数
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname)

        return pool

    def _forkLog(self, datas: dict):
        """Fork日志信息到一个独立的数据表

        :datas: 包含日志信息的数据，dict类型
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
        columns_value = list()  # 多个COLUMN VALUE field

        # timestamp/id value
        timestamp = "{ts_field}".format(ts_field=datas.get(self.column_time))
        id_ = "{id_name}".format(id_name=datas.get(self.column_id))

        # 构建COLUMN NAME、COLUMN VALUE和COLUMN MARK
        # # 构建COLUMN NAME（固有列）
        columns_name = "{column_time}, {column_id}".format(
            column_time=self.column_time,  # 固有的时间戳列
            column_id=self.column_id,  # 固有的ID列
        )
        # # 构建COLUMN VALUE
        columns_value.append(timestamp)  # 固有的时间戳列
        columns_value.append(id_)  # 固有的ID列
        # # 构建COLUMN MARK
        columns_value_mark: str = "%s, %s"  # 固有的MARK（时间戳和ID）
        # # 完善COLUMN NAME、COLUMN VALUE和COLUMN MARK
        # 完善COLUMN NAME
        columns_name += ", {column_name}".format(column_name='message')
        # 完善COLUMN VALUE
        columns_value.append(datas['fields']['message'].get('value', str()))
        # 完善COLUMN MARK
        columns_value_mark += ", %s"
        others = ['level', 'source', 'logpath']
        for key in others:
            if key in datas['fields'].keys():
                # 完善COLUMN NAME
                columns_name += ", {column_name}".format(column_name=key)
                # 完善COLUMN VALUE
                columns_value.append(datas['fields'][key].get('value', str()))
                # 完善COLUMN MARK
                columns_value_mark += ", %s"

        # 构建SQL语句
        SQL = ("INSERT INTO {schema_name}.{table_name} ({column_name}) "
               "VALUES ({column_value});").format(
                   # SCHEMA.TABLE
                   schema_name=self.log_schema,
                   table_name=self.log_table,
                   # COLUMN NAME
                   column_name=columns_name,
                   # COLUMN VALUE
                   column_value=columns_value_mark)

        return SQL, columns_value

    def _reconnect(self):
        """重开与TimescaleDB的连接"""
        if not self.database._closed:
            self.database.close()
        self.connect()

    def connect(self):
        """从连接池中获取一个TimescaleDB连接对象

        :returns: TimescaleDB连接对象

        """
        while True:
            try:
                pool_obj = self._createPool()
                self.database = pool_obj.connection()
                break
            except OperationalError as err:
                log.error(
                    "TimescaleDB Connection error: {error}".format(error=err))
            except AttributeError:
                log.error("Pool failed, please check configuration.")
            except Exception as err:
                log.error(err)

            time.sleep(2)

    def createSchema(self, schema: str):
        """创建Schema

        :schema: 要创建的Schema名

        """
        # 构建SQL语句
        SQL = "CREATE SCHEMA {schema};".format(schema=schema)

        # 执行SQL语句
        try:
            cursor = self.database.cursor()
            cursor.execute(SQL)
            self.database.commit()
        except DuplicateSchema as warn:
            log.warning("Duplicate schema: {warn}".format(warn=warn))
        except (OperationalError, InterfaceError):
            log.error('Reconnect to the TimescaleDB ...')
            self._reconnect()
        except Exception as err:
            log.error(err)

    def createTable(self, schema: str, table: str, columns: dict):
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
                columns_name = ("{curr_columns}, "
                                "{new_columns} {attr_1} {attr_2}").format(
                                    curr_columns=columns_name,
                                    new_columns=column,
                                    attr_1='DOUBLE PRECISION',
                                    attr_2='NULL')
            elif type_ in ['str']:
                # str类型的数据默认存储为VARCHAR
                columns_name = ("{curr_columns}, "
                                "{new_columns} {attr_1} {attr_2}").format(
                                    curr_columns=columns_name,
                                    new_columns=column,
                                    attr_1='VARCHAR',
                                    attr_2='NULL')
        SQL = "CREATE TABLE {schema_name}.{table_name} ({columns});".format(
            schema_name=schema, table_name=table, columns=columns_name)

        # 执行SQL语句
        try:
            cursor = self.database.cursor()
            cursor.execute(SQL)
            self.database.commit()
        except DuplicateTable as warn:
            log.warning("Create table: {text}".format(text=warn))
        except (OperationalError, InterfaceError):
            log.error('Reconnect to the TimescaleDB ...')
            self._reconnect()
        except Exception as err:
            log.error(err)

    def createHypertable(self, schema: str, hypertable: str, columns: dict):
        """创建Hypertable

        :schema: 使用的Schema名
        :hypertable: 要创建的Hypertable名
        :columns: Column名及其数据类型
                  columns = {
                                'column1': 'int',
                                'column2': 'float',
                                'column3': 'str',
                                ... ...
                            }

        """
        # 构建SQL语句
        columns_name = ("{column_time} TIMESTAMP NOT NULL, "
                        "{column_id} VARCHAR NOT NULL").format(
                            column_time=self.column_time,
                            column_id=self.column_id)
        for column, type_ in columns.items():
            if type_ in ['int', 'float']:
                # int和float类型的数据默认存储为DOUBLE PRECISION
                columns_name = ("{curr_columns}, "
                                "{new_columns} {attr_1} {attr_2}").format(
                                    curr_columns=columns_name,
                                    new_columns=column,
                                    attr_1='DOUBLE PRECISION',
                                    attr_2='NULL')
            elif type_ in ['str']:
                # str类型的数据默认存储为VARCHAR
                columns_name = ("{curr_columns}, "
                                "{new_columns} {attr_1} {attr_2}").format(
                                    curr_columns=columns_name,
                                    new_columns=column,
                                    attr_1='VARCHAR',
                                    attr_2='NULL')

        SQL = "CREATE TABLE {schema_name}.{table_name} ({columns});".format(
            schema_name=schema, table_name=hypertable, columns=columns_name)
        SQL_HYPERTABLE = ("SELECT create_hypertable("
                          "'{schema_name}.{table_name}', "
                          "'{column_time}');").format(
                              schema_name=schema,
                              table_name=hypertable,
                              column_time=self.column_time)
        # 执行SQL语句
        try:
            cursor = self.database.cursor()
            cursor.execute(SQL)
            cursor.execute(SQL_HYPERTABLE)
            self.database.commit()
        except InvalidSchemaName as warn:  # Schema不存在
            # 尝试创建Schema
            log.error("Undefined schema: {text}".format(text=warn))
            log.info('Creating schema ...')
            self.createSchema(schema=schema)
        except DuplicateTable as warn:  # Hypertable已存在
            log.warning("Duplicate hypertable: {text}".format(text=warn))
        except (OperationalError, InterfaceError):
            log.error('Reconnect to the TimescaleDB ...')
            self._reconnect()
        except Exception as err:
            log.error(err)

    def addColumn(self, schema: str, table: str, datas: dict):
        """添加Column

        :schema: 使用的Schema名
        :table: 使用的Table名
        :datas: Column名及其数据类型
                datas = {
                            'column1': {
                                'type': 'int'
                                ... ...
                            },
                            'column2': {
                                'type': 'float',
                                ... ...
                            },
                            'column3': {
                                'type': 'str'
                                ... ...
                            }
                            ... ...
                        }

        """
        try:
            cursor = self.database.cursor()
            # TimescaleDB限制了一次只能新增一列
            for key, value in datas.items():
                # 处理没指定type的情况
                if 'type' in value.keys():
                    # 构建SQL语句
                    if value['type'] in ['int', 'float']:
                        # int和float类型的数据默认存储为DOUBLE PRECISION
                        data_type = 'DOUBLE PRECISION'
                    else:
                        data_type = 'VARCHAR'

                    SQL = (
                        "ALTER TABLE {schema_name}.{table_name} "
                        "ADD COLUMN IF NOT EXISTS {column_name} {data_type};"
                    ).format(schema_name=schema,
                             table_name=table,
                             column_name=key,
                             data_type=data_type)

                    # 执行SQL语句
                    cursor.execute(SQL)
                    self.database.commit()
                else:
                    log.error(
                        'Cannot add column, value type is not specified.')
        except (OperationalError, InterfaceError):
            log.error('Reconnect to the TimescaleDB ...')
            self._reconnect()
        except Exception as err:
            log.error(err)

    def insertData(self, datas: list or dict):
        """向数据表批量插入数据
        参数datas类型是list时需要保证其中每个dict的'schema'.'table'一致，且每个dict的'fields'的key相同

        解析参数datas，构建列名字符串columns_name: str和每列的值column_value: list，
        然后将column_value: list组合成一个大列表columns_value: list，最后构建SQL语句进行批量插入

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
        # SQL_MSG、COLUMN NAME和COLUMN VALUE
        SQL_MSG = str()  # Log message SQL statement
        columns_name = str()  # COLUMN NAME field
        column_value = list()  # 单个COLUMN VALUE field
        columns_value = list()  # 多个COLUMN VALUE field

        if isinstance(datas, (list, types.GeneratorType)):
            # schema.table value和timestamp/id value
            schema = "{schema_name}".format(schema_name=datas[0].get('schema'))
            table = "{table_name}".format(table_name=datas[0].get('table'))
            timestamp = "{ts_field}".format(ts_field=datas[0].get('timestamp'))
            id_ = "{id_name}".format(id_name=datas[0].get(self.column_id))

            # 构建COLUMN NAME、COLUMN VALUE和COLUMN MARK
            # # 构建COLUMN NAME（固有列）
            columns_name = "{column_time}, {column_id}".format(
                column_time=self.column_time,  # 固有的时间戳列
                column_id=self.column_id,  # 固有的ID列
            )
            # # 构建COLUMN VALUE
            column_value.append(timestamp)  # 固有的时间戳列
            column_value.append(id_)  # 固有的ID列
            # # 构建COLUMN MARK
            columns_value_mark: str = "%s, %s"  # 固有的MARK（时间戳和ID）
            # # 完善COLUMN NAME和COLUMN MARK
            for column, data in datas[0]['fields'].items():
                # 完善COLUMN NAME
                columns_name += ", {column_name}".format(column_name=column)
                # 完善COLUMN MARK
                columns_value_mark += ", %s"  # 确定MARK的不定长部分长度
            # 完善COLUMN VALUE
            for data in datas:
                # 检索处理日志信息，如果'message'和'level'都是data['fields']的key
                if 'message' in data['fields'].keys():
                    SQL_MSG, msgs_columns_value = self._forkLog(datas=data)
                for data in data['fields'].values():
                    # 构建COLUMN VALUE
                    column_value.append(data['value'])
                # 合并多个VALUE
                columns_value.append(column_value)
                # 初始化column_value，防止两个data的value混淆
                column_value = list()  # 单个COLUMN VALUE field
                column_value.append(timestamp)  # 固有的时间戳列
                column_value.append(id_)  # 固有的ID列
        elif isinstance(datas, dict):
            # schema.table value和timestamp/id value
            schema = "{schema_name}".format(schema_name=datas.get('schema'))
            table = "{table_name}".format(table_name=datas.get('table'))
            timestamp = "{ts_field}".format(ts_field=datas.get('timestamp'))
            id_ = "{id_name}".format(id_name=datas.get(self.column_id))

            # 构建COLUMN NAME、COLUMN VALUE和COLUMN MARK
            # # 构建COLUMN NAME（固有列）
            columns_name = "{column_time}, {column_id}".format(
                column_time=self.column_time,  # 固有的时间戳列
                column_id=self.column_id,  # 固有的ID列
            )
            # # 构建COLUMN VALUE
            column_value.append(timestamp)  # 固有的时间戳列
            column_value.append(id_)  # 固有的ID列
            # # 构建COLUMN MARK
            columns_value_mark: str = "%s, %s"  # 固有的MARK（时间戳和ID）
            # # 完善COLUMN NAME、COLUMN VALUE和COLUMN MARK
            for column, data in datas['fields'].items():
                # 完善COLUMN NAME
                columns_name += ", {column_name}".format(column_name=column)
                # 完善COLUMN VALUE
                column_value.append(data['value'])
                # 完善COLUMN MARK
                columns_value_mark += ", %s"
            # 合并多个COLUMN VALUE
            columns_value.append(column_value)

            # 检索处理日志信息，如果'message'和'level'都是datas['fields']的key
            if 'message' in datas['fields'].keys():
                SQL_MSG, msgs_columns_value = self._forkLog(datas=datas)
        else:
            log.error("Data type error, 'datas' must be list or dict.")

        # 构建SQL语句
        SQL = ("INSERT INTO {schema_name}.{table_name} ({column_name}) "
               "VALUES ({column_value});").format(
                   # SCHEMA.TABLE
                   schema_name=schema,
                   table_name=table,
                   # COLUMN NAME
                   column_name=columns_name,
                   # COLUMN VALUE
                   column_value=columns_value_mark)

        # 执行SQL语句
        try:
            cursor = self.database.cursor()
            tag = 0
            cursor.executemany(SQL, columns_value)
            tag = 1
            if SQL_MSG:
                cursor.executemany(SQL_MSG, msgs_columns_value)
            self.database.commit()
            log.debug('Data inserted successfully.')
        except UndefinedTable as warn:
            # 数据库中不存在指定数据表，尝试创建
            log.error('Undefined table: {text}'.format(text=warn))
            # 尝试创建Schema
            log.info('Creating schema ...')
            # # 根据tag（指明了try中运行到哪一步）决定参数值
            curr_schema = schema if tag == 0 else self.log_schema
            curr_table = table if tag == 0 else self.log_table
            self.createSchema(schema=curr_schema)
            # 尝试创建Hypertable
            log.info('Creating hypertable ...')
            columns = dict()
            # # 根据datas的类型取到它的'fields'
            cache = datas if isinstance(datas, dict) else datas[0]
            # # 根据tag（指明了try中运行到哪一步）决定参数值
            for key, value in cache['fields'].items():
                if tag == 1:
                    if key in ['message', 'level']:
                        columns.update({key: value['type']})
                else:
                    columns.update({key: value['type']})
            self.createHypertable(schema=curr_schema,
                                  hypertable=curr_table,
                                  columns=columns)
            # 尝试再次写入数据
            cursor = self.database.cursor()
            cursor.executemany(SQL, columns_value)
            if SQL_MSG:
                cursor.executemany(SQL_MSG, msgs_columns_value)
            self.database.commit()
            log.debug('Data inserted successfully.')
        except UndefinedColumn as warn:
            # 数据表中缺少某个Column，动态添加
            log.warning('Undefined column: {text}'.format(text=warn))
            # 尝试添加Column
            log.info('Adding column ...')
            # # 根据tag（指明了try中运行到哪一步）决定参数值
            curr_schema = schema if tag == 0 else self.log_schema
            curr_table = table if tag == 0 else self.log_table
            # # 根据datas的类型取到它的'fields'
            cache = datas if isinstance(datas, dict) else datas[0]
            # # 根据tag（指明了try中运行到哪一步）决定参数值
            for key in cache['fields'].keys():
                if tag == 1 and key not in ['message', 'level']:
                    cache.pop(key, None)
            self.addColumn(schema=curr_schema,
                           table=curr_table,
                           datas=cache['fields'])
            # 尝试再次写入数据
            cursor = self.database.cursor()
            cursor.executemany(SQL, columns_value)
            self.database.commit()
            log.debug('Data inserted successfully.')
        except (OperationalError, InterfaceError):
            log.error('Reconnect to the TimescaleDB ...')
            self._reconnect()
        except Exception as err:
            log.error(err)

    def queryData(self,
                  schema: str,
                  table: str,
                  column='*',
                  order='id',
                  limit=5):
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
               "ORDER BY {order} DESC LIMIT {limit};").format(
                   column=column,
                   schema_name=schema,
                   table_name=table,
                   order=order,
                   limit=limit)

        # 执行SQL语句
        try:
            cursor = self.database.cursor()
            cursor.execute(SQL)
            result = cursor.fetchall()
            self.database.commit()
        except (UndefinedTable, UndefinedColumn) as warn:
            log.error('Query error: {text}'.format(text=warn))
        except (OperationalError, InterfaceError):
            log.error('Reconnect to the TimescaleDB ...')
            self._reconnect()
        except Exception as err:
            log.error(err)

        return result

    def use4test(self):
        """Use for test"""
        # 构建SQL语句
        SQL = "SHOW timezone;"

        # 执行SQL语句
        try:
            cursor = self.database.cursor()
            cursor.execute(SQL)
            data = cursor.fetchall()
            self.database.commit()
            print(data)
        except (OperationalError, InterfaceError):
            log.error('Reconnect to the TimescaleDB ...')
            self._reconnect()
        except Exception as err:
            log.error(err)


if __name__ == "__main__":
    # 导入测试数据
    import sys
    sys.path.append('..')
    from tools.genesis import genesis

    # 创建与TimescaleDB的连接
    confile = '../conf/conf.toml'
    conf = toml.load(confile)
    client = TimescaleWrapper(conf)

    # 运行简单测试方法
    while True:
        client.use4test()
        time.sleep(1)

        # 一条数据有578列
        datas = genesis()
        # 测试插入数据
        client.insertData(datas=datas)
        print('Insert Data')

        # 测试查询数据
        columns = 'timestamp,id'
        conf = conf['database']['timescale']
        result = client.queryData(column=columns,
                                  schema=datas.get('schema'),
                                  table=datas.get('table'),
                                  order=conf['table'].get('column_time'),
                                  limit=1)
        print('Query result: \n{result}\n'.format(result=result))
