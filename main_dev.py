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

import asyncio
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
# 因为使用了多进程，需要Queue进行跨进程通信，而queue.Queue是进程内通信队列
from multiprocessing import Process, Queue

import toml

from plugins.parser_postgresql import parse_data
from utils.database_wrapper import PostgresqlWrapper
from utils.log_wrapper import setup_logging
from utils.mqtt_wrapper_dev import MqttClient
from utils.mqtt_wrapper import subscriber

logger = logging.getLogger('DataWizard.main')

# 加载配置文件
confile = 'conf/config.toml'
config = toml.load(confile)
# 程序配置
app_conf = config.get('app', dict())
app_name = app_conf.get('name', 'DataWizard')
app_version = app_conf.get('version', None)
# [main] - Wizard配置
main_conf = config.get('main', dict())
# # 线程池中每个topic的最大worker数，如果未配置则取值当前进程可用CPU核心数x2
number = main_conf.get('number', len(os.sched_getaffinity(0)) * 2)
# [source] - 数据源配置
source_conf = config.get('source', dict())
source_select = source_select = source_conf.get('select', 'mqtt')
source_entity = source_conf.get(source_select.lower(), dict())
topics = source_entity.get('topics', list())
# [cache] - 缓存配置
cache_conf = config.get('cache', dict())
cordon = cache_conf.get('cordon', 5000)
# [storage] - 数据存储配置
storage_conf = storage_conf = config.get('storage', dict())
storage_select = storage_select = storage_conf.get('select', 'postgresql')
storage_entity = storage_conf.get(storage_select.lower(), dict())
# 根据topic数量动态构造数据缓存队列的字典
queues = [Queue(maxsize=cordon) for _ in range(len(topics))]
queue_dict = dict(zip(topics, queues))
# 构建数据存储客户端
if storage_select.lower() in ['postgresql']:
    database = PostgresqlWrapper(conf=storage_entity)
# [log] - Log记录器配置
log_conf = config.get('log', dict())
setup_logging(log_conf)


def convert(raw_data):
    """解码并加载数据
    :returns: data

    """
    data_str = raw_data.decode('UTF-8')
    data = json.loads(data_str)

    return data


def persistence(topic):
    """数据持久化

    :topic: topic name

    """
    while True:
        # 获取原始数据
        topic_queue = queue_dict.get(topic, Queue(maxsize=cordon))
        data_bytes = topic_queue.get()
        size = topic_queue.qsize()
        logger.info(
            'Get data from queue ({topic}), queue size = {size}'.format(
                topic=topic, size=size))

        # 解析原始数据
        datas = convert(data_bytes)
        result = parse_data(flow=storage_select,
                            config=storage_conf,
                            datas=datas)

        print('---------', datas)
        print('---------', result)

        #  # 持久化数据
        #  start_time = time.time()
        #  # # 调用新版数据插入函数
        #  for res in result:
        #      if res:
        #          database.insert(material=res)
        #  # # 调用旧版数据插入函数
        #  # database.insert_oldgen(datas)
        #  end_time = time.time()
        #  logger.info('Persistence time cost: {cost}s'.format(cost=end_time -
        #                                                      start_time))

        # 存活线程计数
        logger.info("Currently active threads = {count}".format(
            count=threading.active_count()))

        # 队列已满则阻塞
        if topic_queue.full():
            logger.error(
                'Queue {name} is full, so it is blocking'.format(name=topic))


def start_source():
    """启动数据源客户端获取数据"""
    logger.info('Get data from {}'.format(source_select.upper()))
    # 1
    #  if source_select.lower() in ['mqtt']:
    #      subscriber(queues=queue_dict)
    # 2
    mqtt = MqttClient(conf=source_entity, queue_dict=queue_dict)
    if source_select.lower() in ['mqtt']:
        mqtt.get()


def start_wizard_threadpool():
    """启动持久化函数 -- 线程池版"""
    # 生成任务列表
    tasks = topics * number
    # max_workers大小和任务列表长度须一致，否则不能在一个周期内完成所有任务
    with ThreadPoolExecutor(max_workers=len(tasks),
                            thread_name_prefix='Wizard') as executor:
        executor.map(persistence, tasks, chunksize=len(topics))


def start_wizard_thread():
    """启动持久化函数 -- 多线程版"""
    for topic in topics:
        for num in range(1, number + 1):
            task = threading.Thread(target=persistence,
                                    args=(topic, ),
                                    name='Wizard-{}'.format(num))
            task.start()


async def main():
    """主函数"""
    logger.info('{name}({version}) start running'.format(name=app_name,
                                                         version=app_version))

    loop = asyncio.get_running_loop()

    while True:
        await loop.run_in_executor(None, start_source)
        await loop.run_in_executor(None, start_wizard_thread)
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
    #  loop = asyncio.get_event_loop()
    #  loop.create_task(main())
    #  loop.run_forever()
