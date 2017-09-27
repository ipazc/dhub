#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from time import sleep
import requests

__author__ = 'Iván de Paz Centeno'


class APIWrapper(object):

    def __init__(self, token, api_url=None, token_info=None):
        if api_url is None:
            api_url = "http://localhost:5000"

        self.api_url = api_url
        self.token = token

        if token_info is None:
            self._update_token_info()
        else:
            self.token_info = token_info

    def _update_token_info(self):
        try:
            token_info = requests.get("{}/tokens/{}".format(self.api_url, self.token), params={'_tok': self.token}).json()
        except Exception as ex:
            print(ex)
            raise Exception("Backend could not be contacted!")

        self.token_info = token_info

    def _get_json(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        data = dict(extra_data)
        data['_tok'] = self.token

        response = requests.get("{}/{}".format(self.api_url, rel_url), json=json_data, params=data)

        while response.status_code == 429:
            sleep(2)
            response = requests.get("{}/{}".format(self.api_url, rel_url), json=json_data, params=data)

        if response.status_code not in [200, 201]:
            raise Exception("Error while retrieving data.")

        return response.json()

    def _get_binary(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.get("{}/{}".format(self.api_url, rel_url), json=json_data, params=data)

        while response.status_code == 429:
            sleep(2)
            response = requests.get("{}/{}".format(self.api_url, rel_url), json=json_data, params=data)

        return response.content

    def _put_binary(self, rel_url, extra_data=None, binary=None):
        if binary is None:
            binary = b""

        if extra_data is None:
            extra_data = {}

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.put("{}/{}".format(self.api_url, rel_url), params=data, data=binary)
        while response.status_code == 429:
            sleep(2)
            response = requests.put("{}/{}".format(self.api_url, rel_url), params=data, data=binary)

        return response.json()

    def _post_json(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.post("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)
        while response.status_code == 429:
            sleep(2)
            response = requests.post("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)

        return response.json()

    def _patch_json(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.patch("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)
        while response.status_code == 429:
            sleep(2)
            response = requests.patch("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)
        return response.json()

    def _delete_json(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.delete("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)
        while response.status_code == 429:
            sleep(2)
            response = requests.delete("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)
        return response.json()
