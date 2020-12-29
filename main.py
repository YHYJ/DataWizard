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
from queue import Queue
from threading import Thread

import toml

from utils.log_wrapper import setupLogging
from utils.mqtt_wrapper import MqttWrapper
from utils.timescale_wrapper_forklog import TimescaleWrapper


class Wizard(object):
    """Data Wizard."""
    def __init__(self, conf):
        """Initialize.

        :conf: 总配置信息

        """
        # 数据缓存队列
        self.data_queue = Queue()

        # [main] - Dizard配置
        main_conf = conf.get('main', dict())
        # # 进程或线程数
        self.number = main_conf.get('number') if main_conf.get(
            'number', 0) > 0 else os.cpu_count()
        data_source = main_conf.get('data_source', 'mqtt')
        data_storage = main_conf.get('data_storage', 'timescale')

        # [source] - 数据来源配置
        source_conf = conf['source'].get(data_source, dict())
        if data_source == 'mqtt':
            self.mqtt = MqttWrapper(source_conf, self.data_queue)

        # [storage] - 数据去处配置
        storage_conf = conf['storage'].get(data_storage, dict())
        if data_storage == 'timescale':
            self.database = TimescaleWrapper(storage_conf)

        # [log] - 日志记录器配置
        self.logger = setupLogging(conf['log'])

    def persistence(self, serial):
        """数据持久化

        :serial: 线程序列号

        """
        while True:
            data_bytes = self.data_queue.get()
            qsize = self.data_queue.qsize()
            if qsize >= 6000:
                break
            data_str = data_bytes.decode('UTF-8')
            data_dict = json.loads(data_str)
            start = time.time()
            self.database.insertData(data_dict)
            end = time.time()
            self.logger.info(("Thread-{num} got data, queue size = {size} "
                              "<-> Time cost = {tc}s").format(num=serial,
                                                              size=qsize,
                                                              tc=end - start))

    def wizard(self):
        """Main."""
        self.mqtt.subMessage()

        for num in range(1, self.number + 1):
            task = Thread(target=self.persistence, args=(num, ))
            task.start()


if __name__ == "__main__":
    confile = './conf/conf.toml'
    conf = toml.load(confile)
    wizard = Wizard(conf)
    wizard.wizard()
