#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests

__author__ = 'Iván de Paz Centeno'


class APIWrapper(object):

    def __init__(self, token, api_url=None):
        if api_url is None:
            api_url = "http://localhost:5000"

        self.api_url = api_url
        self.token = token
        self._update_token_info()

    def _update_token_info(self):
        try:
            token_info = requests.get("{}/tokens/{}".format(self.api_url, self.token), params={'_tok': self.token}).json()
        except Exception as ex:
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
        return response.json()

    def _get_binary(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.get("{}/{}".format(self.api_url, rel_url), json=json_data, params=data)
        return response.content

    def _put_binary(self, rel_url, extra_data=None, binary=None):
        if binary is None:
            binary = b""

        if extra_data is None:
            extra_data = {}

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.post("{}/{}".format(self.api_url, rel_url), params=data, data=binary)
        return response.json()

    def _post_json(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        data = dict(extra_data)
        data['_tok'] = self.token
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
        return response.json()

    def _delete_json(self, rel_url, extra_data=None, json_data=None):
        if extra_data is None:
            extra_data = {}

        if json_data is None:
            json_data = {}

        data = dict(extra_data)
        data['_tok'] = self.token
        response = requests.delete("{}/{}".format(self.api_url, rel_url), params=data, json=json_data)
        return response.json()
