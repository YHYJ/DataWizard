#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: mqtt_wrapper_dev.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2022-01-11 15:39:18

Description:
"""

import json
import logging
import time

import paho.mqtt.client as Mqtt
import toml

logger = logging.getLogger('DataWizard.utils.mqtt_wrapper')


class MqttClient(object):
    """Get/put data via MQTT bridge"""
    def __init__(self, conf, queue_dict):
        """Initialization

        :conf: Configuration info
        :queue_dict: data queue, only needed when calling the get method
                     list {Queue1, Queue2, Queue3}

        """
        # MQTT
        self._hostname = conf.get('host', '127.0.0.1')
        self._port = conf.get('port', 1883)
        self._username = conf.get('username', None)
        self._password = conf.get('password', None)
        self._clientid = clientid = conf.get('clientid', str())
        self._clean = conf.get('clean', False if clientid else True)
        self._topics = conf.get('topics', list())
        self._qos = conf.get('qos', 2)
        self._keepalive = conf.get('keepalive', 60)

        # {Queue1, Queue2, Queue3}
        self.queue_dict = queue_dict

        # MQTT client
        self._client = None
        self._connect()

    def _connect(self):
        """Connect to MQTT"""
        self._client = Mqtt.Client(client_id=self._clientid,
                                   clean_session=self._clean)
        self._client.username_pw_set(self._username, self._password)

        self._client.on_connect = self.__on_connect
        self._client.on_disconnect = self.__on_disconnect
        self._client.on_publish = self.__on_publish
        self._client.on_subscribe = self.__on_subscribe
        self._client.on_message = self.__on_message

        try:
            self._client.connect(host=self._hostname,
                                 port=self._port,
                                 keepalive=self._keepalive)
        except Exception as e:
            logger.error("Connection error - {text}".format(text=e))

    @staticmethod
    def __on_connect(client, userdata, flags, reasonCode, properties=None):
        """Called when the broker respo nds to our connection request

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
            logger.info('MQTT bridge connected')
        else:
            logger.error(
                'Connection error, reasonCode = {}'.format(reasonCode))
            client.disconnect()

    @staticmethod
    def __on_disconnect(client, userdata, reasonCode, properties=None):
        """Called when the client disconnects from the broker

        :client: client instance that is calling the callback
        :userdata: user data of any type
        :reasonCode: the disconnection result
                     The reasonCode parameter indicates the disconnection state
        :properties: the MQTT v5.0 properties returned from the broker
                     For MQTT v3.1 and v3.1.1, properties = None

        """
        logger.info("Disconnection with reasonCode = {}".format(reasonCode))

    @staticmethod
    def __on_publish(client, userdata, mid):
        """Called when a message that was to be sent using the publish()
        call has completed transmission to the broker

        For messages with QoS levels:
            0 -- this simply means that the message has left the client
            1 or 2 -- this means that the appropriate handshakes have completed
        Even if the publish() call returns success,
        it doesn't always mean that the message has been sent.

        :client: client instance that is calling the callback
        :userdata: user data of any type
        :mid: matches the mid variable returned from the corresponding
              publish() call, to allow outgoing messages to be tracked

        """
        logger.info('Published success, mid = {}'.format(mid))

    @staticmethod
    def __on_subscribe(client, userdata, mid, granted_qos, properties=None):
        """Called when the broker responds to a subscribe request

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
        logger.info('Subscribed success, mid = {} granted_qos = {} '.format(
            mid, granted_qos))

    def __on_message(self, client, userdata, message):
        """Called when a message has been received on a topic

        :client: client instance that is calling the callback
        :userdata: user data of any type
        :message: an instance of MQTTMessage
                  This is a class with members topic, payload, qos, retain.

        """
        # 获取实际topic名
        topic = message.topic
        # 获取配置中的topic名（即队列名）
        for queue_name in self._topics:
            msg = message.payload
            logger.info(
                'Received message from ({topic}) topic'.format(topic=topic))

            topic_queue = self.queue_dict.get(queue_name)
            topic_queue.put(msg)
            size = topic_queue.qsize()
            logger.info('Put the message in the queue, queue size = {size}'.format(
                size=size))

            # 队列大小检测
            if topic_queue.full():
                logger.error(
                    'Queue {name} is full, so it is blocking'.format(name=queue_name))

    def get(self):
        """Subscribe to message from MQTT bridge"""
        try:
            for topic in self._topics:
                self._client.subscribe(topic=topic, qos=self._qos)
            self._client.loop_start()
        except Exception as e:
            logger.error('Unable to subscribe to data - {text}'.format(text=e))

    def put(self, data):
        """Publish message to MQTT bridge

        :data: message

        """
        try:
            self._client.loop_start()
            payload = json.dumps(data)
            for topic in self._topics:
                self._client.publish(topic=topic,
                                     payload=payload,
                                     qos=self._qos)
        except Exception as e:
            logger.error('Unable to subscribe to data - {text}'.format(text=e))
