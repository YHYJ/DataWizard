#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: main_use4test.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-06 16:06:47

Description: 将data和message从缓存(redis, mqtt ...)持久化到数据库(PostgreSQL)

测试用，主要区别为：
1. 使用配置文件中指定的线程数而不设定最小值
2. 使用多线程而非线程池
"""

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue

import toml

from plugins.parser_postgresql import parse_data
from utils.database_wrapper import PostgresqlWrapper
from utils.log_wrapper import setup_logging
from utils.mqtt_wrapper import subscriber

logger = logging.getLogger('DataWizard.main')


class Wizard(object):
    """Data Wizard"""
    def __init__(self, config):
        """Initialize

        :config: 总配置信息

        """
        # [main] - Wizard配置
        main_conf = config.get('main', dict())
        self.number = main_conf.get('number')

        # [source] - 数据源配置
        source_conf = config.get('source', dict())
        self.source_select = source_select = source_conf.get('select', 'mqtt')
        source_entity = source_conf.get(source_select.lower(), dict())
        self.topics = source_entity.get('topics', list())

        # [cache] - 缓存配置
        cache_conf = config.get('cache', dict())
        self.cordon = cache_conf.get('cordon', 5000)

        # [storage] - 数据存储配置
        self.storage_conf = storage_conf = config.get('storage', dict())
        self.storage_select = storage_select = storage_conf.get(
            'select', 'postgresql')
        storage_entity = storage_conf.get(storage_select.lower(), dict())

        # 根据topic数量动态构造数据缓存队列的字典
        queues = [Queue() for _ in range(len(self.topics))]
        self.queue_dict = dict(zip(self.topics, queues))

        # 构建数据存储客户端
        if storage_select.lower() in ['postgresql']:
            self.database = PostgresqlWrapper(conf=storage_entity)

        # [log] - Log记录器配置
        log_conf = config.get('log', dict())
        setup_logging(log_conf)

    @staticmethod
    def convert(raw_data):
        """解码并加载数据
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
            # time.sleep(0.1)
            data_bytes = queue.get()
            qsize = queue.qsize()
            logger.info(
                'Get data from queue ({topic}), queue size = {size}'.format(
                    topic=topic, size=qsize))

            datas = self.convert(data_bytes)
            # result = parse_data(flow=self.storage_select,
            #                     config=self.storage_conf,
            #                     datas=datas)
            start_time = time.time()
            self.database.insert_oldgen(datas)
            # for res in result:
            #     if res:
            #         self.database.insert(material=res)
            end_time = time.time()
            logger.info('Persistence time cost: {cost}s'.format(cost=end_time -
                                                                start_time))

            logger.info("Currently active thrends = {count}".format(
                count=threading.active_count()))

            if qsize >= self.cordon:
                logger.error(
                    'Queue ({name}) is too big, empty it'.format(name=topic))
                queue.queue.clear()

    def start_source(self):
        """启动数据源客户端获取数据"""
        logger.info('Get data from {}'.format(self.source_select.upper()))
        if self.source_select.lower() in ['mqtt']:
            subscriber(queues=self.queue_dict)

    def start_wizard(self):
        """Main"""
        # 生成任务列表
        tasks = self.topics * self.number
        # max_workers大小和任务列表长度须一致，否则不能在一个周期内完成所有任务
        with ThreadPoolExecutor(max_workers=len(tasks),
                                thread_name_prefix='Wizard') as executor:
            executor.map(self.persistence, tasks, chunksize=len(self.topics))

    def start_wizard_thread(self):
        """启动数据源客户端获取数据"""
        logger.info('Get data from {}'.format(self.source_select.upper()))
        for topic in self.topics:
            for num in range(1, self.number + 1):
                task = threading.Thread(target=self.persistence, args=(topic, ), name='Wizard-{}'.format(num))
                task.start()


if __name__ == "__main__":
    confile = 'conf/config.toml'
    config = toml.load(confile)
    wizard = Wizard(config)

    app_conf = config.get('app', dict())
    app_name = app_conf.get('name', 'DataWizard')
    app_version = app_conf.get('version', None)
    logger.info('{name}({version}) start running'.format(name=app_name,
                                                         version=app_version))

    # 创建并启动进程
    source = Process(target=wizard.start_source)
    wizard = Process(target=wizard.start_wizard_thread)
    source.start()
    wizard.start()
    source.join()
    wizard.join()