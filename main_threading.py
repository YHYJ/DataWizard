#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: main_threading.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-06 16:06:47

Description: 将data和message从缓存(redis, mqtt ...)持久化到数据库(TimescaleDB)

使用threading模块开启多线程
"""

import json
import logging
import os
import time
from queue import Queue
from threading import Thread

import toml

from utils.log_wrapper import setup_logging
from utils.mqtt_wrapper import MqttWrapper
from utils.timescale_wrapper import TimescaleWrapper

logger = logging.getLogger('DataWizard.main')


class Wizard(object):
    """Data Wizard."""
    def __init__(self, conf):
        """Initialize.

        :conf: 总配置信息

        """
        # [main] - Dizard配置
        main_conf = conf.get('main', dict())
        # # 进程或线程数
        self.number = main_conf.get('number') + os.cpu_count(
        ) if main_conf.get('number', 0) > 0 else os.cpu_count()
        data_source = main_conf.get('data_source', 'mqtt')
        data_storage = main_conf.get('data_storage', 'timescaledb')

        # 主要配置部分
        source_conf = conf['source'].get(data_source, dict())
        storage_conf = conf['storage'].get(data_storage, dict())

        # 根据topic数量动态构造数据缓存队列的字典
        self.topics = source_conf.get('topics', list())
        queues = [Queue() for _ in range(len(self.topics))]
        self.queue_dict = dict(zip(self.topics, queues))

        # [source] - 数据来源配置
        if data_source == 'mqtt':
            self.mqtt = MqttWrapper(source_conf, self.queue_dict)

        # [storage] - 数据去处配置
        if data_storage == 'timescaledb':
            self.database = TimescaleWrapper(storage_conf)

        # [log] - Log记录器配置
        setup_logging(conf['log'])

    def convert(self, raw_data):
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

            data = self.convert(data_bytes)
            _start = time.time()
            self.database.insert(data)
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
        for topic in self.topics:
            for num in range(1, self.number + 1):
                task = Thread(target=self.persistence,
                              args=(topic, ),
                              name='Wizard-{}'.format(num))
                task.start()


if __name__ == "__main__":
    logger.info('Action')

    confile = './conf/conf.toml'
    conf = toml.load(confile)
    wizard = Wizard(conf)

    wizard.start_mqtt()
    wizard.start_wizard()
