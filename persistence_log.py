#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: persistence_log.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-06 16:06:47

Description: 将log从缓存(redis, mqtt ...)持久化到数据库(TimescaleDB)

"""

import json
import logging
import os
import time
from queue import Queue
from threading import Thread

import paho.mqtt.client as Mqtt
import toml

from utils.log_wrapper import setupLogging
from utils.timescale_wrapper_forklog import TimescaleWrapper

log = logging.getLogger("DataWizard.main")


class Wizard(object):
    """Data Wizard."""
    def __init__(self, conf):
        """Initialize.

        :conf: 总配置信息

        """
        setupLogging(conf['log'])
        # 数据的来源和去处
        self.data_source: str = conf['source'].get('data_source', 'mqtt')
        self.data_storage: str = conf['storage'].get('data_storage',
                                                     'timescale')

        # 数据缓存队列
        self.data_queue = Queue()

        # 线程数
        self.threads: int = conf.get('threads') if conf.get(
            'threads', 0) > 0 else os.cpu_count()

        # 数据来源配置
        if self.data_source == 'mqtt':
            mqtt_conf: dict = conf['source'].get('mqtt', dict())
            self._hostname: str = mqtt_conf.get('host', '127.0.0.1')
            self._port: int = mqtt_conf.get('port', 1883)
            self._username: str = mqtt_conf.get('username', None)
            self._password: str = mqtt_conf.get('password', None)
            self._client_id: str = mqtt_conf.get('client_id', str())
            self._topics: list = mqtt_conf.get('topics', list())
            self._qos: int = mqtt_conf.get('qos', 2)
            self._keepalive: int = mqtt_conf.get('keepalive', 60)

            self.mqtt = None
            self.connectMqtt()

        # 数据去处配置
        self.database = TimescaleWrapper(conf['storage'].get(
            self.data_storage, dict()))

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
            log.info("Successfully connected to {text}.".format(
                text=self.data_source))
        except Exception as err:
            log.error("Connection error: {text}".format(text=err))

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
            log.info('MQTT bridge connected.')
        else:
            log.error('Connection error, reasonCode = {text}.'.format(
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
        log.info(
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
        log.info('Subscribed success, mid = {mid} granted_qos = {qos}.'.format(
            mid=mid, qos=granted_qos))

    def __on_message(self, client, userdata, message):
        """called when a message has been received on a topic.

        :client: client instance that is calling the callback
        :userdata: user data of any type
        :message: an instance of MQTTMessage
                  This is a class with members topic, payload, qos, retain.

        """
        msg = message.payload
        print('Subscribe to data: {}'.format(msg))
        self.data_queue.put(msg, block=False)

    def subMessage(self):
        """Subscribe to data from MQTT bridge."""
        try:
            self.mqtt.loop_start()
            for topic in self._topics:
                self.mqtt.subscribe(topic=topic, qos=self._qos)
        except Exception as err:
            log.error(err)

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
            n = time.time()
            self.database.insertData(data_dict)
            time.sleep(0.01)  # 阻塞0.01~0.02秒效果更好
            o = time.time()
            log.info(("Thread {num} got data, long(queue) = {size} "
                      "<-> time cost = {tc}").format(num=serial,
                                                     size=qsize,
                                                     tc=o - n))

    def wizard(self):
        """Main."""
        self.subMessage()

        for serial in range(1, self.threads + 1):
            task = Thread(target=self.persistence, args=(serial, ))
            #  task.setDaemon(True)
            task.start()


if __name__ == "__main__":
    confile = './conf/conf.toml'
    conf = toml.load(confile)
    wizard = Wizard(conf)
    wizard.wizard()
