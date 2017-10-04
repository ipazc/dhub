#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# dhub
# Copyright (C) 2017 Iván de Paz Centeno <ipazc@unileon.es>.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 3
# of the License or (at your option) any later version of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

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

