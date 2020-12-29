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

import paho.mqtt.client as Mqtt
import toml

from utils.log_wrapper import setupLogging
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
        self.number = main_conf.get('number') if main_conf.get(
            'number', 0) > 0 else os.cpu_count()
        data_source = main_conf.get('data_source', 'mqtt')
        data_storage = main_conf.get('data_storage', 'timescale')

        # [source] - 数据来源配置
        source_conf = conf['source'].get(data_source, dict())
        if data_source == 'mqtt':
            self._hostname = source_conf.get('host', '127.0.0.1')
            self._port = source_conf.get('port', 1883)
            self._username = source_conf.get('username', None)
            self._password = source_conf.get('password', None)
            self._client_id = source_conf.get('client_id', str())
            self._topics = source_conf.get('topics', list())
            self._qos = source_conf.get('qos', 2)
            self._keepalive = source_conf.get('keepalive', 60)

            self.mqtt = None
            self.connectMqtt()

        # [storage] - 数据去处配置
        storage_conf = conf['storage'].get(data_storage, dict())
        if data_storage == 'timescale':
            self.database = TimescaleWrapper(storage_conf)

        # [log] - 日志记录器配置
        self.logger = setupLogging(conf['log'])

        # 数据缓存队列
        self.data_queue = Queue()

    def connectMqtt(self):
        """Connect to MQTT."""
        self.mqtt = Mqtt.Client(client_id=self._client_id)
        self.mqtt.username_pw_set(self._username, self._password)

        self.mqtt.on_connect = self.__on_connect
        self.mqtt.on_disconnect = self.__on_disconnect
        self.mqtt.on_subscribe = self.__on_subscribe
        self.mqtt.on_message = self.__on_message

        try:
            self.mqtt.connect(host=self._hostname,
                              port=self._port,
                              keepalive=self._keepalive)
        except Exception as err:
            self.logger.error("Connection error: {text}".format(text=err))

    def __on_connect(self,
                     client,
                     userdata,
                     flags,
                     reasonCode,
                     properties=None):
        """called when the broker respo nds to our connection request.

        :client: client instance that is calling the callback
        :userdata: user data of any type
        :flags: a dict that contains response flags from the broker
        :reasonCode: the connection result
                     May be compared to interger
        :properties: the MQTT v5.0 properties returned from the broker
                     For MQTT v3.1 and v3.1.1, properties = None

        The value of reasonCode indicates success or not:
            0: Connection successful
            1: Connection refused - incorrect protocol version
            2: Connection refused - invalid client identifier
            3: Connection refused - server unavailable
            4: Connection refused - bad username or password
            5: Connection refused - not authorised
            6-255: Currently unused

        """
        if reasonCode == 0:
            self.logger.info('MQTT bridge connected.')
        else:
            self.logger.error('Connection error, reasonCode = {text}.'.format(
                text=reasonCode))
            client.disconnect()

    def __on_disconnect(self, client, userdata, reasonCode, properties=None):
        """called when the client disconnects from the broker.

        :client: client instance that is calling the callback
        :userdata: user data of any type
        :reasonCode: the disconnection result
                     The reasonCode parameter indicates the disconnection state
        :properties: the MQTT v5.0 properties returned from the broker
                     For MQTT v3.1 and v3.1.1, properties = None

        """
        self.logger.info(
            "Disconnection with reasonCode = {text}.".format(text=reasonCode))

    def __on_subscribe(self,
                       client,
                       userdata,
                       mid,
                       granted_qos,
                       properties=None):
        """called when the broker responds to a subscribe request.

        :client: client instance that is calling the callback
        :userdata: user data of any type
        :mid: matches the mid variable returned from the corresponding
              publish() call, to allow outgoing messages to be tracked
        :granted_qos: list of integers that give the QoS level the broker has
                      granted for each of the different subscription requests
        :properties: the MQTT v5.0 properties returned from the broker
                     For MQTT v3.1 and v3.1.1, properties = None

        Expected signature for MQTT v3.1.1 and v3.1 is:
            callback(client, userdata, mid, granted_qos, properties=None)

        and for MQTT v5.0:
            callback(client, userdata, mid, reasonCodes, properties)

        """
        self.logger.info(
            'Subscribed success, mid = {mid} granted_qos = {qos}.'.format(
                mid=mid, qos=granted_qos))

    def __on_message(self, client, userdata, message):
        """called when a message has been received on a topic.

        :client: client instance that is calling the callback
        :userdata: user data of any type
        :message: an instance of MQTT message
                  This is a class with members topic, payload, qos, retain.

        """
        msg = message.payload
        self.data_queue.put(msg)

    def subMessage(self):
        """Subscribe to data from MQTT bridge."""
        try:
            self.mqtt.loop_start()  # 不能用loop_forever
            for topic in self._topics:
                self.mqtt.subscribe(topic=topic, qos=self._qos)
        except Exception as err:
            self.logger.error(err)

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
        self.subMessage()

        for num in range(1, self.number + 1):
            task = Thread(target=self.persistence, args=(num, ))
            task.start()


if __name__ == "__main__":
    confile = './conf/conf.toml'
    conf = toml.load(confile)
    wizard = Wizard(conf)
    wizard.wizard()
