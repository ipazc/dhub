#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from queue import Queue

__author__ = 'Iván de Paz Centeno'

class SmartUpdater(object):

    def __init__(self):
        self.content_queue = Queue()
        self.update_queue = Queue()

    def append_update(self):
