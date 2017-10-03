#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
from time import sleep
from pyzip import PyZip

__author__ = 'Iván de Paz Centeno'

pool = ThreadPoolExecutor(4)
UPDATE_INTERVAL = 2 #seconds

class AsyncSmartUpdater(object):

    def __init__(self, server_info, api_wrapper_owner):
        self.server_info = server_info
        self.api_wrapper_owner = api_wrapper_owner
        self.content_put_queue = Queue()
        self.element_update_queue = Queue()
        self.__exit = False
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._thread_func)
        self.thread.start()

    @property
    def _exit(self):
        with self.lock:
            exit_value = self.__exit
        return exit_value

    @_exit.setter
    def _exit(self, do_exit=True):
        with self.lock:
            self.__exit = do_exit

    def _thread_func(self):
        ps = self.server_info['Page-Size']
        queues = {'binary':[self.content_put_queue], 'json':[self.element_update_queue]}

        while not self._exit:

            # Let's gather all the updates from the queue to serialize them
            sleep(UPDATE_INTERVAL)

            for request_kind, queues_list in queues.items():

                for queue in queues_list:

                    gathered_elements = []

                    while queue.qsize() > 0:
                        gathered_elements.append(queue.get())

                        # This is the smart action: we combine several requests into one
                        if len(gathered_elements) > ps:
                            pool.submit(self.__do_update, request_kind, gathered_elements)
                            gathered_elements = []

                    if len(gathered_elements) > 0:
                        pool.submit(self.__do_update, request_kind, gathered_elements)



    def __do_update(self, request_kind, elements):

        if len(elements) > 0:
            url = elements[0][0]
        else:
            url = None

        kwargs_list = {}
        for element in elements:
            kwargs_list[element[1]] = element[2]

        if len(kwargs_list) == 0:
            return None

        if request_kind == "json":
            self.api_wrapper_owner._patch_json(url, extra_data=None, json_data={'elements': kwargs_list})
        else: # request_kind == "binary":
            self.api_wrapper_owner._put_binary(url, extra_data=None, binary=PyZip(kwargs_list).to_bytes())

    def queue_update(self, url, element_id, kwargs):
        self.element_update_queue.put([url, element_id, kwargs])

    def queue_content_update(self, url, element_id, content):
        self.content_put_queue.put([url, element_id, content])

    def stop(self):
        self.__exit = True
        self.thread.join(10)