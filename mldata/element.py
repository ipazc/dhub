#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mldata.wrapper.api_wrapper import APIWrapper

__author__ = 'Iván de Paz Centeno'


class Element(APIWrapper):

    def __init__(self, title, description, tags, http_ref, id=None, dataset_owner=None, token=None,
                 binary_interpreter=None, token_info=None):
        super().__init__(token, token_info=token_info)
        self.data = {'title': title, 'description': description, 'tags': tags, 'http_ref': http_ref}
        self.has_content = False
        self.dataset_owner = dataset_owner
        self.binary_interpreter = binary_interpreter
        self.token = token
        self._id = id

    def get_title(self):
        return self.data['title']

    def get_description(self):
        return self.data['description']

    def get_tags(self):
        return self.data['tags']

    def get_ref(self):
        return self.data['http_ref']

    def get_id(self):
        return self._id

    def set_title(self, new_title):
        self.data['title'] = new_title

    def set_description(self, new_description):
        self.data['description'] = new_description

    def set_tags(self, new_tags):
        self.data['tags'] = new_tags

    def set_ref(self, new_http_ref):
        self.data['http_ref'] = new_http_ref

    def get_content(self, interpret=True):
        if not self.has_content:
            return None

        content = self._get_binary("datasets/{}/elements/{}/content".format(self.dataset_owner.get_url_prefix(), self._id))

        if self.binary_interpreter is not None and interpret:
            content = self.binary_interpreter.interpret(content)

        return content

    def set_content(self, content, interpret=True):
        if self.binary_interpreter is not None and interpret:
            content = self.binary_interpreter.deinterpret(content)

        result = self._put_binary("datasets/{}/elements/{}/content".format(self.dataset_owner.get_url_prefix(), self._id), binary=content)
        self.refresh()
        return result

    def update(self):
        self._patch_json("datasets/{}/elements/{}".format(self.dataset_owner.get_url_prefix(), self._id),
                         json_data=self.data)
        self.refresh()

    def __str__(self):
        return "{} {}".format(self._id, str(self.data))

    @classmethod
    def from_dict(cls, definition, dataset_owner, token, binary_interpreter=None, token_info=None):

        element = cls(definition['title'], definition['description'], definition['tags'],
                      definition['http_ref'], id=definition['_id'], token=token,
                      binary_interpreter=binary_interpreter, dataset_owner=dataset_owner, token_info=token_info)

        element.comments_count = definition['comments_count']
        element.has_content = definition['has_content']
        return element

    def refresh(self):
        definition = self._get_json("datasets/{}/elements/{}".format(self.dataset_owner.get_url_prefix(), self.get_id()))

        self.data = {k: definition[k] for k in ['title', 'description', 'tags', 'http_ref']}
        self.comments_count = definition['comments_count']
        self.has_content = definition['has_content']
