#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
from os.path import expanduser

__author__ = 'Iván de Paz Centeno'

class MlDataRc(object):
    def __init__(self):
        home = expanduser("~")
        self.token_lookup = {}
        self.backend = "http://localhost:5555/"
        try:
            with open(os.path.join(home, ".mldatarc")) as f:
                rc = json.load(f)

            self.token_lookup = rc['token_lookup']
            self.backend = rc['backend']
        except FileNotFoundError as ex:
            pass

    def lookup_token(self, token_str):
        if token_str is None:
            token_str = "default"

        return self.token_lookup[token_str]

    def get_backend(self):
        return self.backend

mldatarc = MlDataRc()