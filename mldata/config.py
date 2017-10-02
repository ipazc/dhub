#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime

__author__ = 'Iván de Paz Centeno'


CACHE_TIME = 60  # seconds for caching elements.

def now():
    return datetime.datetime.now()

def segments(list, size):
    if size <= 0: return []
    list_len = len(list)
    split_count = list_len // size
    extra_split = list_len % size > 0

    for x in range(split_count):
        yield list[x*size: (x+1)*size]

    if extra_split:
        yield list[(split_count)*size:]

