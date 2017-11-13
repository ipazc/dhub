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

from concurrent.futures import ThreadPoolExecutor
from time import sleep
import requests
from dhub.dhubrc import dhubrc

__author__ = 'Iván de Paz Centeno'

TIMEOUT=30
request_pool = ThreadPoolExecutor(2)


def retry(func_wrap):
    def fun(obj, *args, **kwargs):
        tries = -1
        result = None
        last_exception = None

        while result is None and tries < 2:

            try:
                result = func_wrap(obj, *args, **kwargs)
            except Exception as e:
                last_exception = e

            tries += 1

        if tries >= 2:
            raise last_exception

        return result
    return fun


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
            sv_info = request_pool.submit(requests.get, "{}/server".format(self.api_url), params={'_tok': self.token})
            token_info = request_pool.submit(requests.get, "{}/tokens/{}".format(self.api_url, self.token), params={'_tok': self.token})
        except Exception as ex:
            print(ex)
            raise Exception("Backend could not be contacted!")

        self.token_info = token_info.result().json()
        self.server_info = sv_info.result().json()

    @retry
    def __do_json_request(self, method, rel_url, extra_data=None, json_data=None, binary_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        if rel_url.startswith("/"):
            rel_url = rel_url[1:]

        data = dict(extra_data)
        data['_tok'] = self.token

        if binary_data is not None:
            response = method("{}/{}".format(self.api_url, rel_url), data=binary_data, params=data, timeout=TIMEOUT)
        else:
            response = method("{}/{}".format(self.api_url, rel_url), json=json_data, params=data, timeout=TIMEOUT)

        while response.status_code == 429:
            sleep(2)
            if binary_data is not None:
                response = method("{}/{}".format(self.api_url, rel_url), data=binary_data, params=data, timeout=TIMEOUT)
            else:
                response = method("{}/{}".format(self.api_url, rel_url), json=json_data, params=data, timeout=TIMEOUT)

        if response.status_code not in [200, 201]:
            raise Exception("Failed to communicate with backend: {}".format(response.content.decode()))

        return response

    def _get_json(self, rel_url, extra_data=None, json_data=None):
        return self.__do_json_request(requests.get, rel_url, extra_data, json_data).json()

    def _get_binary(self, rel_url, extra_data=None, json_data=None):
        return self.__do_json_request(requests.get, rel_url, extra_data, json_data).content

    def _put_binary(self, rel_url, extra_data=None, binary=None):
        return self.__do_json_request(requests.put, rel_url, extra_data, binary_data=binary).json()

    def _post_json(self, rel_url, extra_data=None, json_data=None):
        return self.__do_json_request(requests.post, rel_url, extra_data, json_data).json()

    def _patch_json(self, rel_url, extra_data=None, json_data=None):
        return self.__do_json_request(requests.patch, rel_url, extra_data, json_data).json()

    def _delete_json(self, rel_url, extra_data=None, json_data=None):
        return self.__do_json_request(requests.delete, rel_url, extra_data, json_data).json()
