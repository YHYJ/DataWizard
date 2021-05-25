#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: sub.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-06 16:06:47

Description: 从MQTT Server订阅消息

使用方法：`python sub.py`

"""

import json
import time

import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, reasonCode, properties=None):
    """连接后事件"""
    if reasonCode == 0:
        print('MQTT bridge connected')
    else:
        print('Connect Error status {}'.format(reasonCode))
        client.disconnect()


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    """代理响应订阅请求后事件"""
    print('Subscribed success, mid = {} granted_qos = {} '.format(
        mid, granted_qos))


def on_disconnect(client, userdata, reasonCode, properties=None):
    """断开连接后事件"""
    print('Disconnection with reasonCode = {}'.format(reasonCode))
    client.loop_stop()


def on_message(client, userdata, message):
    """接收到数据后事件"""
    result = {
        'Timestamp': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'Topic': message.topic,
        'Payload': json.loads(message.payload.decode())
    }
    print(result)


if __name__ == "__main__":
    # connect参数
    hostname = "127.0.0.1"
    port = 1883
    alive = 60

    # subscribe参数
    qos = 0
    topics = ['topic', 'topic_1']

    # 创建客户端
    client = mqtt.Client()

    # 注册事件
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect

    # 连接到服务器
    client.connect(
        host=hostname,
        port=port,
        keepalive=alive,
    )

    # 订阅信息
    for topic in topics:
        client.subscribe(topic, qos)

    # 守护连接状态
    client.loop_forever()
