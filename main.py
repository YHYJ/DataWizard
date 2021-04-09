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

from plugins.parser_postgresql import parse_system_monitor
from utils.database_wrapper import PostgresqlWrapper
from utils.log_wrapper import setup_logging
from utils.mqtt_wrapper import MqttWrapper

logger = logging.getLogger('DataWizard.main')


class Wizard(object):
    """Data Wizard."""
    def __init__(self, conf):
        """Initialize.

        :conf: 总配置信息

        """
        # [main] - Wizard配置
        main_conf = conf.get('main', dict())
        # # 进程或线程数
        self.number = main_conf.get('number') + os.cpu_count(
        ) if main_conf.get('number', 0) > 0 else os.cpu_count()
        self.data_source = data_source = main_conf.get('data_source', 'mqtt')
        self.data_storage = data_storage = main_conf.get(
            'data_storage', 'postgresql')

        # 主要配置部分
        self.source_conf = source_conf = conf.get('source', dict())
        mqtt_conf = source_conf.get(data_source, dict())
        self.storage_conf = storage_conf = conf.get('storage', dict())
        postgresql_conf = storage_conf.get(data_storage, dict())

        # 根据topic数量动态构造数据缓存队列的字典
        self.topics = mqtt_conf.get('topics', list())
        self.heartbeat_topics = mqtt_conf.get('heartbeat_topics', list())
        queues = [Queue() for _ in range(len(self.topics))]
        self.queue_dict = dict(zip(self.topics, queues))

        # [source] - 数据来源配置
        if data_source == 'mqtt':
            self.mqtt = MqttWrapper(mqtt_conf, self.queue_dict)

        # [storage] - 数据去处配置
        if data_storage.lower() == 'postgresql':
            self.database = PostgresqlWrapper(postgresql_conf)

        # [log] - Log记录器配置
        setup_logging(conf['log'])

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
            if topic in self.heartbeat_topics:
                result = parse_system_monitor(flow=self.data_storage,
                                              config=self.storage_conf,
                                              datas=datas)
                schema = result.get('schema', str())
                table = result.get('table', str())
                sql = result.get('sql', str())
                data = result.get('data', list())
                self.database.insert_nextgen(schema=schema,
                                             table=table,
                                             sql=sql,
                                             data=data)
            else:
                self.database.insert(datas)
            _end = time.time()
            logger.info(
                ("Got the data, "
                 "Queue ({name}) size = {size} "
                 "<--> Time cost = {cost}s").format(name=topic,
                                                    size=qsize,
                                                    cost=_end - _start))
            if qsize >= 5000:
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
    logger.info('Action')

    confile = 'conf/conf.toml'
    conf = toml.load(confile)
    wizard = Wizard(conf)

    wizard.start_mqtt()
    wizard.start_wizard()
