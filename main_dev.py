#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: main.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-06 16:06:47

Description: 将data和log从缓存(redis, mqtt ...)持久化到数据库(TimescaleDB)

"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

import toml

from utils.log_wrapper import setup_logging
from utils.mqtt_wrapper import MqttWrapper
from utils.timescale_wrapper_forklog import TimescaleWrapper


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
        data_storage = main_conf.get('data_storage', 'timescale')

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
        if data_storage == 'timescale':
            self.database = TimescaleWrapper(storage_conf)

        # [log] - 日志记录器配置
        self.logger = setup_logging(conf['log'])

    def persistence(self, topic):
        """数据持久化

        :topic: topic name

        """
        while True:
            queue = self.queue_dict.get(topic)
            data_bytes = queue.get()
            qsize = queue.qsize()
            if qsize >= 6000:
                break
            data_str = data_bytes.decode('UTF-8')
            data_dict = json.loads(data_str)
            start = time.time()
            self.database.insert(data_dict)
            end = time.time()
            self.logger.info(("Got the data, "
                              "Queue ({name}) size = {size} "
                              "<--> Time cost = {tc}s").format(name=topic,
                                                               size=qsize,
                                                               tc=end - start))

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
    confile = './conf/conf.toml'
    conf = toml.load(confile)
    wizard = Wizard(conf)

    wizard.start_mqtt()
    wizard.start_wizard()
