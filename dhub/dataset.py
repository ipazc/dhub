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
import csv
import json
import os
from time import sleep
from pyfolder import PyFolder
from pyzip import PyZip
from dhub.config import now, CACHE_TIME, segments
from dhub.element import Element
from dhub.wrapper.api_wrapper import APIWrapper
from dhub.interpreters.interpreter import Interpreter
from dhub.wrapper.smart_updater import AsyncSmartUpdater

__author__ = 'Iván de Paz Centeno'

pool_keys = ThreadPoolExecutor(4)
pool_content = ThreadPoolExecutor(4)


class Dataset(APIWrapper):
    def __init__(self, url_prefix: str, title: str, description: str, reference: str, tags: list, token: str=None,
                 binary_interpreter: Interpreter=None, token_info: dict=None, server_info: dict=None,
                 use_smart_updater: AsyncSmartUpdater=True, owner=None):
        self.data = {}

        self.binary_interpreter = binary_interpreter
        """:type : Interpreter"""

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
        self.data['size'] = 0
        self.elements_count = 0
        self.comments_count = 0
        self.page_cache = {}
        self.last_cache_time = now()
        self.owner = owner

        super().__init__(token, token_info=token_info, server_info=server_info)

        # Server_info is only available after super() init.
        if use_smart_updater:
            self.smart_updater = AsyncSmartUpdater(self.server_info, self)
            """
            :type : AsyncSmartUpdater
            """
        else:
            self.smart_updater = None
            """
            :type : AsyncSmartUpdater
            """

    def __repr__(self):

        return "Dataset {} ({} elements); tags: {}; description: {}; reference: {}; fork_father: {}, fork_count: {}, size: {}".format(self.get_title(), len(self),
                                                                                           self.get_tags(),
                                                                                           self.get_description(),
                                                                                           self.get_reference(),
                                                                                           self.get_fork_father(),
                                                                                           self.get_fork_count(),
                                                                                           self.get_size())


    def set_binary_interpreter(self, binary_interpreter):
        self.binary_interpreter = binary_interpreter

    def get_fork_father(self):
        return self.data['fork_father']

    def get_fork_count(self):
        return self.data['fork_count']

    def get_url_prefix(self):
        return self.data['url_prefix']

    def get_description(self):
        return self.data['description']

    def get_title(self):
        return self.data['title']

    def get_tags(self):
        return self.data['tags']

    def get_size(self):
        return self.data['size']

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
                         json_data={k: v for k, v in self.data.items() if k != "url_prefix"})

    @classmethod
    def from_dict(cls, definition, token, binary_interpreter=None, token_info=None, server_info=None, owner=None):

        dataset = cls(definition['url_prefix'], definition['title'], definition['description'], definition['reference'],
                      definition['tags'], token=token, binary_interpreter=binary_interpreter, token_info=token_info,
                      server_info=server_info, owner=owner)
        dataset.data['fork_count'] = definition['fork_count']
        dataset.data['fork_father'] = definition['fork_father']
        dataset.data['size'] = definition['size']
        dataset.comments_count = definition['comments_count']
        dataset.elements_count = definition['elements_count']
        return dataset

    def add_element(self, title: str, description: str, tags: list, http_ref: str, content, interpret=True) -> Element:

        if type(content) is str:
            # content is a URI
            if not os.path.exists(content):
                raise Exception("content must be a binary data or a URI to a file.")

            with open(content, "rb") as f:
                content_bytes = f.read()

            if self.binary_interpreter is None:
                content = content_bytes
            else:
                content = self.binary_interpreter.cipher(content_bytes)

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

    def add_elements(self, add_element_kwargs_list: list) -> list:
        # we need to preprocess the argument
        post_kwargs = []
        content_list = []
        for element_kwargs in add_element_kwargs_list:
            content = element_kwargs['content']

            if type(content) is str:
                # content is a URI
                if not os.path.exists(content):
                    raise Exception("content must be a binary data or a URI to a file.")

                with open(content, "rb") as f:
                    content_bytes = f.read()

                if self.binary_interpreter is None:
                    content = content_bytes
                else:
                    content = self.binary_interpreter.cipher(content_bytes)

            content_list.append(content)

            post_kwargs.append({'title': element_kwargs['title'],
                                'description': element_kwargs['description'],
                                'tags': element_kwargs['tags'],
                                'http_ref': element_kwargs['http_ref']})

        result = self._post_json("datasets/{}/elements/bundle".format(self.get_url_prefix()), json_data={
            'elements': post_kwargs
        })

        self.refresh()
        elements = [Element.from_dict(element, self, self.token, self.binary_interpreter, token_info=self.token_info,
                                  server_info=self.server_info, smart_updater=self.smart_updater) for element in result]

        for element, content in zip(elements, content_list):
            element.set_content(content)

        return elements

    def _request_segment(self, ids):
        results = self._get_json("datasets/{}/elements/bundle".format(self.get_url_prefix()),
                                 json_data={'elements': ids})

        # Warning: what if 'result' does not have the elements ordered in the same way as 'ids'?
        # Todo: reorder 'elements' to match the order of 'ids'
        elements = [
            Element.from_dict(result, self, self.token, self.binary_interpreter, token_info=self.token_info,
                              server_info=self.server_info, smart_updater=self.smart_updater)
            for result in results
            ]

        future = pool_content.submit(self.__retrieve_segment_contents, ids)

        for element in elements:
            element.content_promise = future

        return elements

    def __retrieve_segment_contents(self, ids):

        if self.smart_updater is not None and self.smart_updater.is_content_update_queued(ids):
            self.smart_updater.wait_for_elements_content_update(ids)

        packet_bytes = self._get_binary("datasets/{}/elements/content".format(self.get_url_prefix()),
                                        json_data={'elements': ids})
        packet = PyZip().from_bytes(packet_bytes)

        return dict(packet)

    def __wait_for_elements_ready(self, ids):
        if self.smart_updater is not None:
            if self.smart_updater.is_element_update_queued(ids):
                self.smart_updater.wait_for_elements_update(ids)

    def __getitem__(self, key):
        options = None

        if type(key) is dict:
            options = key
            if 'slice' in options:
                key = options['slice']
                if (type(key) is int and key < 0) or (type(key) is slice and key.stop < 0):
                    raise ValueError("Negative indexes not allowed when retrieving elements with options. Use filter_iter() instead")

                del options['slice']
            else:
                key = 0

        if type(key) is int:
            if key < 0:
                key += len(self)
            key = slice(key, key + 1, 1)

        elements = []

        if type(key) is slice:
            step = key.step
            start = key.start
            stop = key.stop
            if key.step is None: step = 1
            if key.stop is None: stop = len(self)
            if key.stop < 0: stop = len(self) - stop

            ps = self.server_info['Page-Size']

            ids = []
            for i in range(start, stop, step):
                ids.append(self._get_key(i, options=options))

            self.__wait_for_elements_ready(ids)

            futures = [pool_keys.submit(self._request_segment, segment) for segment in segments(ids, ps)]

            elements = []

            for future in futures:
                elements += future.result()

        elif type(key) is str:
            try:
                self.__wait_for_elements_ready([key])
                response = self._get_json("datasets/{}/elements/{}".format(self.get_url_prefix(), key))
                element = Element.from_dict(response, self, self.token, self.binary_interpreter,
                                            token_info=self.token_info, server_info=self.server_info,
                                            smart_updater=self.smart_updater)
                element.content_promise = pool_content.submit(element._retrieve_content)
                elements = [element]

            except Exception as ex:
                elements = []

        else:
            raise KeyError("Type of key not allowed.")

        if len(elements) > 1:
            result = elements
        elif len(elements) == 1:
            result = elements[0]
        else:
            raise KeyError("{} not found".format(key))

        return result

    def __delitem__(self, key):
        options = None

        if type(key) is dict:
            options = key
            if 'slice' in options:
                key = options['slice']
                if (type(key) is int and key < 0) or (type(key) is slice and key.stop < 0):
                    raise ValueError("Negative indexes not allowed when retrieving elements with options. Use filter_iter() instead")

                del options['slice']
            else:
                key = 0

        if type(key) is int:
            if key < 0:
                key += len(self)
            key = slice(key, key + 1, 1)

        ids = []

        if type(key) is slice:
            step = key.step
            start = key.start
            stop = key.stop
            if key.step is None: step = 1
            if key.stop is None: stop = len(self)
            if key.stop < 0: stop = len(self) - stop

            ps = self.server_info['Page-Size']

            ids = [self._get_key(i, options=options) for i in range(start, stop, step)]

        elif type(key) is str:
                ids = [key]

        if len(ids) > 1:
            result = self._delete_json("datasets/{}/elements/bundle".format(self.get_url_prefix()), json_data={'elements': ids})
        elif len(ids) == 1:
            result = self._delete_json("datasets/{}/elements/{}".format(self.get_url_prefix(), ids[0]))
        else:
            raise KeyError("{} not found".format(key))

        self.refresh()
        return result

    def _get_elements(self, page, filter_options=None):
        return [Element.from_dict(element, self, self.token, self.binary_interpreter, token_info=self.token_info,
                                  server_info=self.server_info, smart_updater=self.smart_updater) for element in
                self._get_json("datasets/{}/elements".format(self.get_url_prefix()), extra_data={'page': page}, json_data={'options': filter_options})]

    def fork(self, new_prefix: str, title: str=None, description: str=None, tags: list=None, reference: str=None,
             destination=None, options: dict=None):

        if destination is None:
            destination = self.owner

        arguments = {
                        'title': title,
                        'description': description,
                        'tags': tags,
                        'url_prefix': new_prefix,
                        'reference': reference,
                        'options': options
                    }
        result = self._post_json("datasets/{}/fork/{}".format(self.get_url_prefix(), destination.token),
                        json_data=arguments)

        destination.refresh()
        return destination[result['url_prefix']]

    def filter_iter(self, options=None, cache_content=False):
        """
        :return:
        """
        if options is None:
            options = {}

        ps = self.server_info['Page-Size']
        number_of_pages = len(self) // ps + int(len(self) % ps > 0)

        buffer = None

        for page in range(number_of_pages):

            if buffer is None:
                buffer = pool_keys.submit(self._get_elements, page, options)

            buffer2 = pool_keys.submit(self._get_elements, page + 1, options)

            elements = buffer.result()

            if len(elements) == 0:
                break

            if cache_content:
                future = pool_content.submit(self.__retrieve_segment_contents, [element.get_id() for element in elements])
            else:
                future = None

            for element in elements:
                if cache_content:
                    element.content_promise = future
                yield element

            buffer = buffer2

    def __iter__(self) -> Element:
        for element in self.filter_iter():
            yield element

    def _get_key(self, key_index, options=None):
        ps = int(self.server_info['Page-Size'])
        key_page = key_index // ps
        index = key_index % ps

        dumped_options = json.dumps(options)

        if json.dumps(options) not in self.page_cache:
            self.page_cache[dumped_options] = {}

        try:
            if (now() - self.last_cache_time).total_seconds() > CACHE_TIME:
                self.page_cache[dumped_options].clear()

            page = self.page_cache[dumped_options][key_page]

        except KeyError as ex:
            # Cache miss
            page = self._get_json("datasets/{}/elements".format(self.get_url_prefix()), extra_data={'page': key_page},
                                                                                        json_data={'options': options})
            self.page_cache[dumped_options][key_page] = page
            self.last_cache_time = now()

        return page[index]['_id']

    def keys(self, page=-1):
        if page == -1:
            data = [self._get_key(i) for i in range(len(self))]
        else:
            data = [element['_id'] for element in
                    self._get_json("datasets/{}/elements".format(self.get_url_prefix()), extra_data={'page': page})]

        return data

    def __len__(self):
        return self.elements_count

    def __str__(self):
        data = dict(self.data)
        data['num_elements'] = len(self)
        url_prefix = data['url_prefix']
        del data['url_prefix']
        order_keys = ['title', 'num_elements', 'reference', 'tags', 'fork_father', 'fork_count', 'description']

        result = "[{}]".format(url_prefix)
        result += " {}"
        content = "{"
        first = True
        for key in order_keys:
            if first:
                first = False
            else:
                content += ", "

            if key == 'tags':
                content += "'{}': {}".format(key, data[key])
            else:
                content += "'{}': \'{}\'".format(key, data[key])

        content += ", '{}': {} MB".format('size', round(int(data['size'])/1024/1024, 3))
        content += "}"

        return result.format(content)

    def save_to_folder(self, folder, metadata_format="json", elements_extension=None, use_numbered_ids=False,
                       only_metadata=False):
        try:
            os.mkdir(folder)
        except Exception as ex:
            pass

        format_saver = {
            "csv": self.__save_csv,
            "json": self.__save_json
        }

        if elements_extension is not None and elements_extension.startswith("."):
            elements_extension = elements_extension[1:]

        if metadata_format not in format_saver:
            raise Exception("format {} for metadata not supported.".format(metadata_format))

        pyfolder = PyFolder(folder)

        print("Collecting elements...")
        metadata = {}
        id = -1
        count = len(self)
        it = -1

        for element in self.filter_iter(cache_content=not only_metadata):
            it += 1

            print("\rProgress: {}%".format(round(it / (count + 0.0001) * 100, 2)), end="", flush=True)

            if use_numbered_ids:
                id += 1
            else:
                id = element.get_id()

            if elements_extension is None:
                element_id = id
            else:
                element_id = "{}.{}".format(id, elements_extension)

            metadata[element_id] = {
                'id': element.get_id(),
                'title': element.get_title(),
                'description': element.get_description(),
                'http_ref': element.get_ref(),
                'tags': element.get_tags(),
            }

            if not only_metadata:
                pyfolder[os.path.join("content", element_id)] = element.get_content(interpret=False)

        print("\rProgress: 100%", end="", flush=True)

        format_saver[metadata_format](pyfolder, metadata)
        print("\nSaved metadata in format {}".format(metadata_format))
        print("Finished")

    def __save_json(self, pyfolder:PyFolder, metadata):
        pyfolder["metadata.json"] = metadata
        data = dict(self.data)
        data['num_elements'] = len(self)
        data['tags'] = data['tags']
        pyfolder["dataset_info.json"] = data

    def __save_csv(self, pyfolder:PyFolder, metadata):
        with open(os.path.join(pyfolder.folder_root, "metadata.csv"), 'w', newline="") as f:
            writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            headers = ['file_name', 'id', 'title', 'description', 'http_ref', 'tags']
            writer.writerow(headers)

            for k, v in metadata.items():
                v['tags'] = "'{}'".format(";".join(v['tags']))
                writer.writerow([k] + [v[h] for h in headers if h != 'file_name'])

        with open(os.path.join(pyfolder.folder_root, "dataset_info.csv"), "w", newline="") as f:
            writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            data = dict(self.data)
            data['num_elements'] = len(self)
            data['tags'] = "'{}'".format(";".join(data['tags']))
            headers = ['url_prefix', 'title', 'description', 'num_elements', 'reference', 'tags']
            writer.writerow(headers)
            writer.writerow([data[h] for h in headers])

    def refresh(self):
        dataset_data = self._get_json("datasets/{}".format(self.get_url_prefix()))
        self.elements_count = dataset_data['elements_count']
        self.comments_count = dataset_data['comments_count']
        self.data = {k: dataset_data[k] for k in ['url_prefix', 'title', 'description', 'reference', 'tags',
                                                  'fork_count', 'fork_father', 'size']}
        self.page_cache = {}  # clearing the cache to avoid errors

    def clear(self):
        ps = self.server_info['Page-Size']

        for segment in segments(self.keys(), ps):
            self._delete_json('datasets/{}/elements/bundle'.format(self.get_url_prefix()), json_data={'elements': segment})

        self.refresh()

    def close(self, force=False):
        if self.smart_updater is not None:
            self.smart_updater.stop(cancel_pending_jobs=force)

    def sync(self, update_size=True):
        if self.smart_updater is not None:
            while self.smart_updater.queues_busy():
                print("\rTasks pending: {}         ".format(self.smart_updater.tasks_pending), end="", flush=True)
                sleep(1)
            print("\rTasks pending: {}         ".format(self.smart_updater.tasks_pending), end="", flush=True)
        print("\n")

        if update_size:
            self.__update_size()

    def load_from_folder(self, folder):
        self.clear()

        pyfolder = PyFolder(folder)

        if "metadata.json" in pyfolder:
            metadata = self.__load_json(pyfolder)
        elif "metadata.csv" in pyfolder:
            metadata = self.__load_csv(pyfolder)
        else:
            raise FileNotFoundError("No metadata.json or metadata.csv found.")

        content_available = "content" in pyfolder

        if not content_available:
            content_folder = None
            print("Warning: elements do not have content associated.")
        else:
            # We don't want PyFolder to interpret the elements of the dataset.
            content_folder = PyFolder(pyfolder["content"].folder_root, interpret=False)

        elements = []
        batch_size = 0
        for key, values in metadata.items():

            element = {k: v for k, v in values.items() if k != "id"}

            if content_available:
                element['content'] = content_folder[key]
                batch_size += len(element['content'])

            elements.append(element)

            if batch_size >= 1*1024*1024*1024:  # each 1 GB, perform upload.
                print("* Uploading elements...")
                self.add_elements(elements)
                batch_size = 0
                elements = []
                self.sync(update_size=False)

        if len(elements) > 0:
            self.add_elements(elements)

        print("Uploading elements...")
        self.sync()
        print("\rFinished uploading.")

    def __load_json(self, pyfolder):
        return pyfolder["metadata.json"]

    def __load_csv(self, pyfolder):
        content = pyfolder["metadata.csv"].split("\n")
        headers = content[0].split(",")
        data = content[1:]

        result = {}
        for line in data:
            values = {k: v for k, v in zip(headers[1:], line[1:])}
            values['tags'] = values['tags'].split(";")
            result[line[0]] = values

        return result

    def __update_size(self):
        size = self._get_json("datasets/{}/size".format(self.get_url_prefix()))
        self.data['size'] = size
