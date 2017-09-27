#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldata.dataset import Dataset

from mldata.wrapper.api_wrapper import APIWrapper

__author__ = 'Iván de Paz Centeno'

class Datasets(APIWrapper):
    def __init__(self, token):
        super().__init__(token)
        self.refresh()

    def __len__(self):
        return len(self.datasets)

    def __getitem__(self, item) -> Dataset:
        result = None
        try:
            index = int(item)
            result = self.values()[index]
        except ValueError as ex:
            pass

        if result is None:
            result = self.datasets[item]

        return result

    def add_dataset(self, url_prefix, title, description, reference, tags):
        result = self._post_json("datasets", json_data={
            'title': title,
            'description': description,
            'tags': tags,
            'url_prefix': url_prefix,
            'reference': reference
        })

        self.refresh()
        return self.datasets[result['url_prefix']]

    def __setitem__(self, key, dataset):
        if dataset.api_url in self.keys():
            return

        self._post_json("tokens/{}/link/{}".format(self.token, dataset.get_url_prefix()))
        self.refresh()

    def __delitem__(self, key):
        try:
            index = int(key)
            item_key = self.keys()[index]
        except ValueError as ex:
            item_key = None
            pass

        if item_key is None:
            if key not in self.keys():
                return
            else:
                item_key = key

        self._delete_json("datasets/{}".format(item_key))
        self.refresh()

    def __iter__(self):
        """
        :rtype: Dataset
        :return:
        """
        for url, d in self.datasets.items():
            yield d

    def keys(self):
        return list(self.datasets.keys())

    def values(self):
        return list(self.datasets.values())

    def refresh(self):
        self.datasets = {d['url_prefix']: Dataset.from_dict(d, self.token, token_info=self.token_info) for d in self._get_json("datasets")}

    def __str__(self):
        return str(self.keys())