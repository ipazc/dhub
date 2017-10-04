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

import os
import json
from os.path import expanduser

__author__ = 'Iván de Paz Centeno'

class DHubRc(object):
    def __init__(self):
        home = expanduser("~")
        self.token_lookup = {}
        self.backend = "http://localhost:5555/"
        retry = False

        try:
            with open(os.path.join(home, ".dhubrc")) as f:
                rc = json.load(f)

            self.token_lookup = rc['token_lookup']
            self.backend = rc['backend']
        except FileNotFoundError as ex:
            retry = True
            pass

        if not retry:
            return

        try:
            with open("/etc/dhub/config.json") as f:
                rc = json.load(f)

            self.token_lookup = rc['token_lookup']
            self.backend = rc['backend']
        except FileNotFoundError as ex:
            retry = True
            pass

    def lookup_token(self, token_str):
        if token_str is None:
            token_str = "default"

        return self.token_lookup[token_str]

    def get_backend(self):
        return self.backend

dhubrc = DHubRc()