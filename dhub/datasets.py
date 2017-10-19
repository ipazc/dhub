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

from dhub.dataset import Dataset
from dhub.dhubrc import dhubrc
from dhub.wrapper.api_wrapper import APIWrapper

__author__ = 'Iván de Paz Centeno'

class Datasets(APIWrapper):
    def __init__(self, token_id=None):

        try:
            token = dhubrc.lookup_token(token_id)
        except KeyError as ex:
            token = token_id

        super().__init__(token)
        self.refresh()

    def __len__(self):
        return len(self.datasets)

    def find_closest(self, item):
        for d in self.keys():
            if item in d:
                return d
        return None

    def __getitem__(self, item) -> Dataset:
        result = None
        try:
            index = int(item)
            result = self.values()[index]
        except ValueError as ex:
            pass

        if result is None:
            # Let's do smart things over here.
            if item not in self.keys():
                closest_name = self.find_closest(item)
                if closest_name is not None:
                    item = closest_name

            result = self.datasets[item]

            if type(result) is dict:
                result = Dataset.from_dict(**result)
                self.datasets[item] = result

        return result

    def add_dataset(self, url_prefix, title, description, reference, tags) -> Dataset:
        result = self._post_json("datasets", json_data={
            'title': title,
            'description': description,
            'tags': tags,
            'url_prefix': url_prefix,
            'reference': reference
        })

        self.refresh()
        return self[result['url_prefix']]

    def __setitem__(self, key, dataset):
        if dataset.api_url in self.keys():
            return

        self._post_json("tokens/{}/link/{}".format(self.token, dataset.get_url_prefix()))
        self.refresh()

    def __delitem__(self, key):
        dataset = self[key]
        self._delete_json("datasets/{}".format(dataset.get_url_prefix()))
        self.refresh()

    def __iter__(self):
        """
        :rtype: Dataset
        :return:
        """
        for key in self.keys():
            yield self[key]

    def keys(self):
        return list(self.datasets.keys())

    def values(self):
        return [s for s in self]

    def refresh(self):
        self.datasets = {d['url_prefix']: dict(definition=d, token=self.token, token_info=self.token_info, server_info=self.server_info, owner=self) for d in self._get_json("datasets")}

    def __str__(self):
        return str(self.keys())

    def __repr__(self):
        return "[{}] {}".format(self.api_url, str(self))

    def __contains__(self, item):
        # Let's do smart things over here.
        if item not in self.keys():
            closest_name = self.find_closest(item)
            if closest_name is not None:
                item = closest_name

        return item in self.keys()