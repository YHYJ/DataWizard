#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: test_mqtt.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-12-29 14:13:50

Description:
"""

import sys

import toml

sys.path.append('..')
from utils.log_wrapper import setupLogging
from utils.mqtt_wrapper import MqttWrapper

confile = '../conf/conf.toml'
conf = toml.load(confile)

setupLogging(conf['log'])

mqtt = MqttWrapper(conf['source']['mqtt'])
mqtt.subMessage()
