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

from time import sleep
import requests
from dhub.dhubrc import dhubrc

__author__ = 'Iván de Paz Centeno'


class APIWrapper(object):

    def __init__(self, token, api_url=None, token_info=None, server_info=None):
        if api_url is None:
             api_url = dhubrc.get_backend()

        if api_url.endswith("/"): api_url = api_url[:-1]
        self.api_url = api_url
        self.token = token

        if token_info is None:
            self._update_token_info() # server_info is also updated here
        else:
            self.token_info = token_info
            self.server_info = server_info

    def _update_token_info(self):
        try:
            sv_info = requests.get("{}/server".format(self.api_url), params={'_tok': self.token}).json()
            token_info = requests.get("{}/tokens/{}".format(self.api_url, self.token), params={'_tok': self.token}).json()
        except Exception as ex:
            print(ex)
            raise Exception("Backend could not be contacted!")

        self.token_info = token_info
        self.server_info = sv_info

    def _get_json(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        if rel_url.startswith("/"):
            rel_url = rel_url[1:]

        data = dict(extra_data)
        data['_tok'] = self.token

        response = requests.get("{}/{}".format(self.api_url, rel_url), json=json_data, params=data)

        while response.status_code == 429:
            sleep(2)
            response = requests.get("{}/{}".format(self.api_url, rel_url), json=json_data, params=data)

        if response.status_code not in [200, 201]:
            raise Exception("Failed to communicate with backend: {}".format(response.content.decode()))

        return response.json()

    def _get_binary(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        if rel_url.startswith("/"):
            rel_url = rel_url[1:]

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.get("{}/{}".format(self.api_url, rel_url), json=json_data, params=data)

        while response.status_code == 429:
            sleep(2)
            response = requests.get("{}/{}".format(self.api_url, rel_url), json=json_data, params=data)

        if response.status_code not in [200, 201]:
            raise Exception("Failed to communicate with backend: {}".format(response.content.decode()))

        return response.content

    def _put_binary(self, rel_url, extra_data=None, binary=None):
        if binary is None:
            binary = b""

        if extra_data is None:
            extra_data = {}

        if rel_url.startswith("/"):
            rel_url = rel_url[1:]

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.put("{}/{}".format(self.api_url, rel_url), params=data, data=binary)
        while response.status_code == 429:
            sleep(2)
            response = requests.put("{}/{}".format(self.api_url, rel_url), params=data, data=binary)

        if response.status_code not in [200, 201]:
            raise Exception("Failed to communicate with backend: {}".format(response.content.decode()))

        return response.json()

    def _post_json(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        if rel_url.startswith("/"):
            rel_url = rel_url[1:]

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.post("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)
        while response.status_code == 429:
            sleep(2)
            response = requests.post("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)

        if response.status_code not in [200, 201]:
            raise Exception("Failed to communicate with backend: {}".format(response.content.decode()))

        return response.json()

    def _patch_json(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        if rel_url.startswith("/"):
            rel_url = rel_url[1:]

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.patch("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)
        while response.status_code == 429:
            sleep(2)
            response = requests.patch("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)

        if response.status_code not in [200, 201]:
            raise Exception("Failed to communicate with backend: {}".format(response.content.decode()))

        return response.json()

    def _delete_json(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        if rel_url.startswith("/"):
            rel_url = rel_url[1:]

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.delete("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)
        while response.status_code == 429:
            sleep(2)
            response = requests.delete("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)

        if response.status_code not in [200, 201]:
            raise Exception("Failed to communicate with backend: {}".format(response.content.decode()))

        return response.json()
