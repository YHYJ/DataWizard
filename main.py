#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: main.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-06 16:06:47

Description: 将data和message从缓存(redis, mqtt ...)持久化到数据库(PostgreSQL)

使用concurrent模块开启异步多线程
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

import toml

from plugins.parser_postgresql import parse_data
from utils.database_wrapper import PostgresqlWrapper
from utils.log_wrapper import setup_logging
from utils.mqtt_wrapper import MqttWrapper

logger = logging.getLogger('DataWizard.main')


class Wizard(object):
    """Data Wizard."""
    def __init__(self, config):
        """Initialize.

        :config: 总配置信息

        """
        # [main] - Wizard配置
        main_conf = config.get('main', dict())
        self.number = main_conf.get('number') if main_conf.get(
            'number', 0) > 0 else os.cpu_count()

        # [source] - 数据源配置
        source_conf = config.get('source', dict())
        source_select = source_conf.get('select', 'mqtt')
        source_entity = source_conf.get(source_select.lower(), dict())

        # [storage] - 数据存储配置
        self.storage_conf = storage_conf = config.get('storage', dict())
        self.storage_select = storage_select = storage_conf.get(
            'select', 'postgresql')
        storage_entity = storage_conf.get(storage_select.lower(), dict())

        # 根据topic数量动态构造数据缓存队列的字典
        self.topics = source_entity.get('topics', list())
        queues = [Queue() for _ in range(len(self.topics))]
        self.queue_dict = dict(zip(self.topics, queues))

        # 构建数据源客户端
        if source_select.lower() in ['mqtt']:
            self.mqtt = MqttWrapper(source_entity, self.queue_dict)

        # 构建数据存储客户端
        if storage_select.lower() in ['postgresql']:
            self.database = PostgresqlWrapper(conf=storage_entity)

        # [log] - Log记录器配置
        log_conf = config.get('log', dict())
        setup_logging(log_conf)

    @staticmethod
    def convert(raw_data):
        """Convert data
        :returns: data

        """
        data_str = raw_data.decode('UTF-8')
        data = json.loads(data_str)

        return data

    def persistence(self, topic):
        """数据持久化

        :topic: topic name

        """
        while True:
            queue = self.queue_dict.get(topic)
            data_bytes = queue.get()
            qsize = queue.qsize()
            datas = self.convert(data_bytes)

            _start = time.time()

            result = parse_data(flow=self.storage_select,
                                config=self.storage_conf,
                                datas=datas)
            #  self.database.insert(datas)
            for res in result:
                if res:
                    schema = res.get('schema', str())
                    table = res.get('table', str())
                    sql = res.get('sql', str())
                    value = res.get('value', list())
                    self.database.insert_nextgen(schema=schema,
                                                 table=table,
                                                 sql=sql,
                                                 value=value)

            _end = time.time()
            logger.info(
                ("Got the data, "
                 "Queue ({name}) size = {size} "
                 "<--> Time cost = {cost}s").format(name=topic,
                                                    size=qsize,
                                                    cost=_end - _start))
            if qsize >= 5000:
                logger.error(
                    'Queue ({name}) is too big, stop it'.format(name=topic))
                break

    def start_mqtt(self):
        """启动Mqtt客户端订阅数据"""
        self.mqtt.sub_message()

    def start_wizard(self):
        """Main."""
        with ThreadPoolExecutor(max_workers=self.number * len(self.topics),
                                thread_name_prefix='Wizard') as executor:
            executor.map(self.persistence,
                         self.topics * self.number,
                         chunksize=len(self.topics))


if __name__ == "__main__":
    confile = 'conf/config.toml'
    config = toml.load(confile)
    wizard = Wizard(config)

    app_conf = config.get('app', dict())
    app_name = app_conf.get('name', 'DataWizard')
    app_version = app_conf.get('version', None)
    logger.info('{name}({version}) start running'.format(name=app_name,
                                                         version=app_version))

    wizard.start_mqtt()
    wizard.start_wizard()
