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
from dhub.config import now, CACHE_TIME

from dhub.wrapper.api_wrapper import APIWrapper
from dhub.wrapper.smart_updater import AsyncSmartUpdater

__author__ = 'Iván de Paz Centeno'


upload_pool = ThreadPoolExecutor(4)


class Element(APIWrapper):

    def __init__(self, title, description, tags, http_ref, id=None, dataset_owner=None, token=None,
                 binary_interpreter=None, token_info=None, server_info=None, smart_updater:AsyncSmartUpdater=None):
        super().__init__(token, token_info=token_info, server_info=server_info)
        self.data = {'title': title, 'description': description, 'tags': tags, 'http_ref': http_ref}
        self.has_content = False
        self.dataset_owner = dataset_owner
        self.binary_interpreter = binary_interpreter
        self.token = token
        self._id = id
        self.cached_content = None
        self.cached_content_time = now()
        self.content_promise = None
        self.smart_updater = smart_updater

    def _retrieve_content(self):

        if self.smart_updater is not None and self.smart_updater.is_content_update_queued([self.get_id()]):
            self.smart_updater.wait_for_elements_content_update([self.get_id()])

        content = self._get_binary("datasets/{}/elements/{}/content".format(self.dataset_owner.get_url_prefix(), self._id))
        return {self._id: content}

    def _upload_content(self, content):
        return self._put_binary("datasets/{}/elements/{}/content".format(self.dataset_owner.get_url_prefix(), self._id), binary=content)

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
        self.update()

    def set_description(self, new_description):
        self.data['description'] = new_description
        self.update()

    def set_tags(self, new_tags):
        self.data['tags'] = new_tags
        self.update()

    def get_tag(self, tag_name):
        tags = self.get_tags()
        result = ""

        for tag in tags:
            if type(tag) is dict:
                if tag_name in tag:
                    result = tag[tag_name]

            elif type(tag) is str:

                if ":" in tag:
                    splits = tag.split(":")
                    if splits[0].strip() == tag_name:
                        result = splits[1].lstrip()
                else:
                    if tag in tag_name:
                        result = tag

            if result != "":
                break

        return result

    def set_tag(self, tag_name, tag_value):
        tags = self.get_tags()
        found = False

        for index, tag in zip(range(len(tags)), tags):
            if tag_name in tag:
                if type(tag) is dict:
                    tag[tag_name] = tag_value
                    found = True

                elif type(tag) is str:
                    tags[index] = '{}: {}'.format(tag_name, tag_value)
                    found = True

                break

        if not found:
            if tag_name is None:
                tags.append(tag_value)
            else:
                tags.append({tag_name: tag_value})

        self.set_tags(tags)
        return found

    def set_ref(self, new_http_ref):
        self.data['http_ref'] = new_http_ref
        self.update()

    def get_content(self, interpret=True):
        if not self.has_content:
            return None

        if self.content_promise is not None:
            self.cached_content = self.content_promise.result()[self._id]
            self.cached_content_time = now()
            self.content_promise = None

        if (now() - self.cached_content_time).total_seconds() > CACHE_TIME:
            self.cached_content = None

        if self.cached_content is None:
            content = self._retrieve_content()[self._id]
        else:
            content = self.cached_content

        if self.binary_interpreter is not None and interpret:
            content = self.binary_interpreter.cipher(content)

        return content

    def set_content(self, content, interpret=True):
        if self.binary_interpreter is not None and interpret:
            content = self.binary_interpreter.decipher(content)
        else:
            if type(content) is not bytes:
                raise Exception("Bytes are required as content.")

        if content == self.cached_content:
            return False

        if self.smart_updater is not None:
            self.smart_updater.queue_content_update("datasets/{}/elements/content".format(self.dataset_owner.get_url_prefix()), self.get_id(), content)
        else:
            upload_pool.submit(self._upload_content, content)

        self.cached_content = content
        self.has_content = True
        self.cached_content_time = now()

        if self.content_promise:
            self.content_promise = None

        return True

    def update(self):
        if self.smart_updater is not None:
            self.smart_updater.queue_update("datasets/{}/elements/bundle".format(self.dataset_owner.get_url_prefix()), self.get_id(), self.data)

        else:
            self._patch_json("datasets/{}/elements/{}".format(self.dataset_owner.get_url_prefix(), self._id),
                             json_data=self.data)
            self.refresh()

    def __str__(self):
        return "{} {}".format(self._id, str(self.data))

    def __repr__(self):
        return "[{}] {}".format(self._id, str(self.data))

    @classmethod
    def from_dict(cls, definition, dataset_owner, token, binary_interpreter=None, token_info=None, server_info=None,
                  smart_updater=None):

        element = cls(definition['title'], definition['description'], definition['tags'],
                      definition['http_ref'], id=definition['_id'], token=token,
                      binary_interpreter=binary_interpreter, dataset_owner=dataset_owner, token_info=token_info, server_info=server_info,
                      smart_updater=smart_updater)

        element.comments_count = definition['comments_count']
        element.has_content = definition['has_content']
        return element


    def refresh(self):
        definition = self._get_json("datasets/{}/elements/{}".format(self.dataset_owner.get_url_prefix(), self.get_id()))

        self.data = {k: definition[k] for k in ['title', 'description', 'tags', 'http_ref']}
        self.comments_count = definition['comments_count']
        self.has_content = definition['has_content']
        self.cached_content = None
