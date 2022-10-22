#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: pub.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-06 16:06:47

Description: 发布消息到MQTT Server

使用方法：`python pub.py <number>`，其中<number>为单位时间内发布消息的数量

"""

import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, reasonCode, properties=None):
    """连接后事件"""
    if reasonCode == 0:
        print('MQTT bridge connected')
    else:
        print('Connect Error status {}'.format(reasonCode))
        client.disconnect()


def on_disconnect(client, userdata, reasonCode, properties=None):
    """断开连接后事件"""
    print('MQTT disconnection, reasonCode = {}'.format(reasonCode))


def on_publish(client, userdata, mid):
    """发布完成后事件
    对于QoS级别为0的消息，这仅代表消息已离开客户端
    对于QoS级别为1或2的消息，这意味着适当的握手已完成
    """
    print('Published success, mid = {}'.format(mid))


def start(tag):
    """Start publish"""
    # 发布数据
    client.loop_start()
    while True:
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        chaos = round(random.uniform(0, 999), 9)
        data = {
            'timestamp': ts,
            'schema': 'alien',
            'table': 'groot',
            'id': 'groot',
            'fields': {
                'x': {
                    'name': 'x',
                    'title': 'X轴',
                    'value': chaos,
                    'type': 'float',
                    'unit': 'mm',
                },
                'message': {
                    'name': 'message',
                    'title': '日志信息',
                    'value': '当前值{}'.format(chaos),
                    'type': 'str',
                    'unit': None,
                },
                'source': {
                    'name': 'source',
                    'title': '日志来源',
                    'value': 'TCP',
                    'type': 'str',
                    'unit': None,
                },
                'level': {
                    'name': 'level',
                    'title': '日志等级',
                    'value': 1,
                    'type': 'int',
                    'unit': None,
                },
                'logpath': {
                    'name': 'logpath',
                    'title': '日志路径',
                    'value': '',
                    'type': 'str',
                    'unit': None,
                },
            }
        }
        payload = json.dumps(data)

        if client._state != 2:
            # 发布/订阅
            for topic in topics:
                client.publish(topic=topic, payload=payload, qos=qos)
        else:
            print('连接失效，进行重连...')
            client.disconnect()
            client.loop_stop()
            print('开始重连')
            client.reconnect()
            client.loop_start()
        time.sleep(1)


if __name__ == "__main__":
    # connect参数
    hostname = "127.0.0.1"
    port = 1883
    alive = 60

    # publish参数
    qos = 0
    topics = ['topic/x', 'topic/y']

    # 创建客户端
    client = mqtt.Client()

    # 注册事件
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    # 连接到服务器
    client.connect(
        host=hostname,
        port=port,
        keepalive=alive,
    )

    numbers = int(sys.argv[1])

    with ThreadPoolExecutor(max_workers=numbers,
                            thread_name_prefix='Mqtt_Publish') as executor:
        executor.map(start, [number for number in range(numbers)],
                     chunksize=len(topics))
