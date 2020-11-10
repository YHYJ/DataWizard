#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: genesis.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-10-30 13:42:34

Description: For testing only
"""

import random
import time
from datetime import datetime

greek = [
    'α', 'β', 'γ', 'δ', 'ϵ', 'ζ', 'η', 'θ', 'ι', 'κ', 'λ', 'μ', 'ν', 'ξ', 'ο',
    'π', 'ρ', 'σ', 'τ', 'υ', 'ϕ', 'χ', 'ψ', 'ω'
]

BLACK_HOLE_MIN = 0
BLACK_HOLE_MAX = 100
LIGHT_YEAR = 2
MAGNITUDE = ['int', 'float', 'str']
NEBULA = ['time', 'kg', 'Nm', '%', 'PH', 'ppm', 'm/s', 'lm']


def genesis():
    """Genesis
    :returns: universe

    """
    world = dict()
    universe = dict()

    timestamp = time.time()
    timestr = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    deviceid = 'Alpha'

    for x in greek:
        for y in greek:
            human = '{}{}'.format(x, y)
            world.update({
                human: {
                    'name':
                    human,
                    'title':
                    human.upper(),
                    'value':
                    round(random.uniform(BLACK_HOLE_MIN, BLACK_HOLE_MAX),
                          LIGHT_YEAR),
                    'type':
                    random.choice(MAGNITUDE),
                    'unit':
                    random.choice(NEBULA)
                }
            })

    universe['timestamp'] = timestr
    universe['schema'] = 'universe'
    universe['table'] = 'earth'
    universe['deviceid'] = deviceid
    universe['fields'] = world

    return universe


if __name__ == "__main__":
    universe = genesis()
    print("{}\n\nlen(universe['fields']) = {}\n".format(
        universe, len(universe['fields'])))
