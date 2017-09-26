#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldata.element import Element
from mldata.wrapper.api_wrapper import APIWrapper

__author__ = 'Iván de Paz Centeno'


class Dataset(APIWrapper):
    def __init__(self, url_prefix, title, description, reference, tags, token=None, binary_interpreter=None):
        self.data = {}
        self.binary_interpreter = binary_interpreter

        if "/" in url_prefix:
            self.data['url_prefix'] = url_prefix
        else:
            if token is not None:
                token_prefix = token.get_prefix()
                self.data['url_prefix'] = '{}/{}'.format(token_prefix, url_prefix)

        self.data['title'] = title
        self.data['description'] = description
        self.data['tags'] = tags
        self.data['reference'] = reference
        self.elements_count = 0
        self.comments_count = 0

        super().__init__(token)

    def set_binary_interpreter(self, binary_interpreter):
        self.binary_interpreter = binary_interpreter

    def get_url_prefix(self):
        return self.data['url_prefix']

    def get_description(self):
        return self.data['description']

    def get_title(self):
        return self.data['title']

    def get_tags(self):
        return self.data['tags']

    def get_reference(self):
        return self.data['reference']

    def set_description(self, new_desc):
        self.data['description'] = new_desc

    def set_title(self, new_title):
        self.data['title'] = new_title

    def set_tags(self, new_tags):
        self.data['tags'] = new_tags

    def set_reference(self, new_reference):
        self.data['reference'] = new_reference

    def update(self):
        self._patch_json("/datasets/{}".format(self.get_url_prefix()),
                         json_data={k:v for k,v in self.data.items() if k != "url_prefix"})

    @classmethod
    def from_dict(cls, definition, token, binary_interpreter=None):

        dataset = cls(definition['url_prefix'], definition['title'], definition['description'], definition['reference'],
                      definition['tags'], token=token, binary_interpreter=binary_interpreter)

        dataset.comments_count = definition['comments_count']
        dataset.elements_count = definition['elements_count']
        return dataset

    def add_element(self, title, description, tags, http_ref, content, interpret=True):

        result = self._post_json("datasets/{}/elements".format(self.get_url_prefix()), json_data={
            'title': title,
            'description': description,
            'tags': tags,
            'http_ref': http_ref,
        })

        self.refresh()

        element = self[result]
        element.set_content(content, interpret)
        return element

    def __getitem__(self, item):
        result = self._get_json("datasets/{}/elements/{}".format(self.get_url_prefix(), item))
        return Element.from_dict(result, self, self.token, self.binary_interpreter)

    def __len__(self):
        return self.elements_count

    def __str__(self):
        return str(self.data)

    def refresh(self):
        dataset_data = self._get_json("datasets/{}".format(self.get_url_prefix()))
        self.data = dataset_data
