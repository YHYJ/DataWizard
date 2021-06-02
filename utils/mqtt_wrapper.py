#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: mqtt_wrapper.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2021-06-02 14:38:24

Description: 与MQTT Broker进行交互
"""

import json
import logging
import time

import paho.mqtt.client as Mqtt
import toml

logger = logging.getLogger('DataWizard.utils.mqtt_wrapper')

# Load configuration file
confile = 'conf/config.toml'
config = toml.load(confile)

# MQTT Broker configuration
# 数据源配置
source_conf = config.get('source', dict())
# MQTT配置
mqtt_conf = source_conf.get('mqtt', dict())
# 连接参数
HOSTNAME = mqtt_conf.get('host', '127.0.0.1')
PORT = mqtt_conf.get('port', 1883)
USERNAME = mqtt_conf.get('username', None)
PASSWORD = mqtt_conf.get('password', None)
CLIENTID = mqtt_conf.get('clientid', str())
CLEAN = mqtt_conf.get('clean', False if CLIENTID else True)
TOPICS = mqtt_conf.get('topics', list())
QOS = mqtt_conf.get('qos', 0)
KEEPALIVE = mqtt_conf.get('keepalive', 60)

# Cache cordon configuration
# 缓存配置
cache_conf = config.get('cache', dict())
# 队列大小警戒线
CORDON = cache_conf.get('cordon', 5000)


def __on_connect(client, userdata, flags, reasonCode):
    if reasonCode == 0:
        logger.info('Connected to MQTT Broker')
    else:
        logger.error('MQTT Broker connection failed, return code = {}'.format(
            reasonCode))


def __on_disconnect(client, userdata, reasonCode):
    logger.info(
        'MQTT Broker disconnection, return code = {}'.format(reasonCode))


def __on_publish(client, userdata, mid):
    logger.info('Published success, mid = {}'.format(mid))


def __on_subscribe(client, userdata, mid, granted_qos):
    logger.info('Subscribed success, mid = {}, granted_qos = {}'.format(
        mid, granted_qos))


# MQTT Broker client
client = Mqtt.Client(client_id=CLIENTID, clean_session=CLEAN)
client.username_pw_set(USERNAME, PASSWORD)
try:
    client.connect(host=HOSTNAME, port=PORT, keepalive=KEEPALIVE)
except Exception as e:
    logger.error('MQTT connection error: {}'.format(e))
client.on_connect = __on_connect
client.on_disconnect = __on_disconnect
client.on_publish = __on_publish
client.on_subscribe = __on_subscribe


def __reconnect():
    """MQTT Broker断线重连函数"""
    client.disconnect()
    client.loop_stop()
    client.reconnect()
    client.loop_start()


def publisher(message):
    """发布者，将消息发布到MQTT Broker指定主题

    :message: 待发布消息
    """
    client.loop_start()

    while True:
        if client._state != 2:
            payload = json.dumps(message)
            for topic in TOPICS:
                result = client.publish(topic=topic, payload=payload, qos=QOS)
                status = result[0]
                if status == 0:
                    logger.info('Send message to topic ({})'.format(topic))
                else:
                    logger.error(
                        'Failed to send message to topic ({})'.format(topic))
        else:
            logger.warning('MQTT connection lost, reconnecting...')
            __reconnect()
        time.sleep(2)


def subscriber(queues):
    """订阅者，从MQTT Broker指定主题订阅消息

    :queues: 队列字典，须topic和queue对应，例如：{'topic': Queue()}
    """
    def on_message(client, userdata, message):
        topic = message.topic
        msg = message.payload
        logger.info(
            'Received message from ({topic}) topic'.format(topic=topic))
        queue = queues.get(topic)
        queue.put(msg)
        qsize = queue.qsize()
        logger.info('Put the message in the queue, queue size = {size}'.format(
            size=qsize))

        # 队列大小检测
        if qsize >= CORDON:
            logger.error(
                'Queue ({name}) is too big, empty it'.format(name=topic))
            queue.queue.clear()

    client.on_message = on_message
    client.loop_start()

    while True:
        if client._state != 2:
            for topic in TOPICS:
                client.subscribe(topic=topic, qos=QOS)
        else:
            logger.warning('MQTT connection lost, reconnecting...')
            __reconnect()
        time.sleep(2)
