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

import concurrent
from concurrent.futures import ThreadPoolExecutor
import os
from queue import Queue, Empty
import threading
from time import sleep
from pyzip import PyZip

__author__ = 'Iván de Paz Centeno'

pool = ThreadPoolExecutor(4)
UPDATE_INTERVAL = 1  # seconds


class AsyncSmartUpdater(object):

    __tasks_pending = 0

    def __init__(self, server_info, api_wrapper_owner):
        self.server_info = server_info
        self.api_wrapper_owner = api_wrapper_owner
        self.content_put_queue = Queue()
        self.element_update_queue = Queue()
        self.queues_priorities = [self.element_update_queue, self.content_put_queue]
        self.queues_cache = {'element_update': {}, 'content_update': {}}
        self.__exit = False
        self.__cancel_pending_jobs = False
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._thread_func, daemon=True)
        self.thread.start()

    @property
    def _exit(self):
        with self.lock:
            exit_value = self.__exit
        return exit_value

    @property
    def tasks_pending(self):
        with self.lock:
            return self.__tasks_pending

    @tasks_pending.setter
    def tasks_pending(self, __tasks_pending):
        with self.lock:
            self.__tasks_pending = __tasks_pending

    def _increase_tasks_pending_counter(self, by=1):
        with self.lock:
            self.__tasks_pending += by

    def _decrease_tasks_pending_counter(self, by=1):
        with self.lock:
            self.__tasks_pending -= by

    @property
    def _cancel_pending_jobs(self):
        with self.lock:
            cancel_pending_jobs = self.__cancel_pending_jobs
        return cancel_pending_jobs

    @_cancel_pending_jobs.setter
    def _cancel_pending_jobs(self, do__cancel_pending_jobs=True):
        with self.lock:
            self.__cancel_pending_jobs = do__cancel_pending_jobs

    def __queue_with_higher_priority_waiting(self, queue):
        """
        Checks if is there any queue with higher priority waiting than the specified queue.
        :param queue: queue to check from
        :return: boolean True if there is at least one queue with higher priority waiting. False otherwise.
        """
        index = 0
        current_queue_index = self.queues_priorities.index(queue)

        while index < current_queue_index and self.queues_priorities[index].qsize() == 0:
            index += 1

        return index != len(self.queues_priorities) and index < current_queue_index

    def __any_queue_with_elements(self):
        return any([queue.qsize() > 0 for queue in self.queues_priorities])

    def is_element_update_queued(self, elements_ids):
        return any(element in self.queues_cache['element_update'] for element in elements_ids)

    def is_content_update_queued(self, elements_ids):
        return any(element in self.queues_cache['content_update'] for element in elements_ids)

    def wait_for_elements_update(self, elements_ids):
        events_list = []

        with self.lock:
            for element_id in elements_ids:
                if element_id not in self.queues_cache['element_update']:
                    continue
                if self.queues_cache['element_update'][element_id] is None:
                    event = threading.Event()
                    self.queues_cache['element_update'][element_id] = event
                else:
                    event = self.queues_cache['element_update'][element_id]

                events_list.append(event)

        launch_exception = None

        try:
            # Now we wait for the events
            for event in events_list:
                event = event
                """
                :type : threading.Event
                """
                if not event.wait(100):
                    raise Exception("Time out while waiting for the elements.")
        except Exception as ex:
            launch_exception = ex
        finally:
            # do some cleanup
            with self.lock:
                for element_id in elements_ids:
                    if element_id in self.queues_cache['element_update']:
                        del self.queues_cache['element_update'][element_id]

        if launch_exception is not None:
            raise launch_exception

    def wait_for_elements_content_update(self, elements_ids):
        events_list = []

        with self.lock:
            for element_id in elements_ids:
                if element_id not in self.queues_cache['content_update']:
                    continue
                if self.queues_cache['content_update'][element_id] is None:
                    event = threading.Event()
                    self.queues_cache['content_update'][element_id] = event
                else:
                    event = self.queues_cache['content_update'][element_id]

                events_list.append(event)

        launch_exception = None

        try:
            # Now we wait for the events
            for event in events_list:
                event = event
                """
                :type : threading.Event
                """
                if not event.wait(100):
                    raise Exception("Time out while waiting for the elements.")
        except Exception as ex:
            launch_exception = ex
        finally:
            # do some cleanup
            with self.lock:
                for element_id in elements_ids:
                    if element_id in self.queues_cache['content_update']:
                        del self.queues_cache['content_update'][element_id]

        if launch_exception is not None:
            raise launch_exception

    def _thread_func(self):
        ps = self.server_info['Page-Size']
        queue_types = {self.content_put_queue: 'binary', self.element_update_queue: 'json'}

        while not self._exit or (not self._cancel_pending_jobs and self.__any_queue_with_elements()):
            # Let's gather all the updates from the queue to serialize them
            sleep(UPDATE_INTERVAL)
            for queue in self.queues_priorities:

                if self.__queue_with_higher_priority_waiting(queue):
                    break  # This forces the loop to start running through the most priority queues again.

                gathered_elements = []
                futures = {}
                queue_empty = False

                while not queue_empty:

                    # Check that queues with highest priority are not waiting
                    if self.__queue_with_higher_priority_waiting(queue):
                        break  # This forces the loop to start running through the most priority queues again.

                    try:
                        element = queue.get(block=False)
                        gathered_elements.append(element)
                    except Empty as ex:
                        queue_empty = True

                    # This is the smart action: we combine several requests into one
                    if len(gathered_elements) > ps-1:
                        futures[pool.submit(self.__do_update, queue_types[queue], gathered_elements)] = len(gathered_elements)
                        gathered_elements = []

                if len(gathered_elements) > 0:
                    futures[pool.submit(self.__do_update, queue_types[queue], gathered_elements)] = len(gathered_elements)

                for future in concurrent.futures.as_completed(futures):
                    self._decrease_tasks_pending_counter(futures[future])

    def queues_busy(self):
        return self.tasks_pending > 0

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
            for [_, element_id, _] in elements:
                event = self.queues_cache['element_update'][element_id]
                """
                :type : threading.Event
                """

                if event is not None:
                    event.set()
                else:
                    with self.lock:
                        del self.queues_cache['element_update'][element_id]

        else: # request_kind == "binary":
            content = PyZip(kwargs_list).to_bytes()
            self.api_wrapper_owner._put_binary(url, extra_data=None, binary=content)
            for [_, element_id, _] in elements:
                event = self.queues_cache['content_update'][element_id]
                """
                :type : threading.Event
                """

                if event is not None:
                    event.set()
                else:
                    with self.lock:
                        del self.queues_cache['content_update'][element_id]

        return True

    def queue_update(self, url, element_id, kwargs):
        self._increase_tasks_pending_counter()
        with self.lock:
            self.queues_cache["element_update"][element_id] = None
        self.element_update_queue.put([url, element_id, kwargs])

    def queue_content_update(self, url, element_id, content):
        self._increase_tasks_pending_counter()
        with self.lock:
            self.queues_cache["content_update"][element_id] = None
        self.content_put_queue.put([url, element_id, content])

    def stop(self, cancel_pending_jobs=False):
        self.__exit = True
        self.__cancel_pending_jobs = cancel_pending_jobs
        self.thread.join(10)