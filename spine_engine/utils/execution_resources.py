######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Engine contributors
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

""" Utilities for managing execution resources such as processes. """
import threading


class ResourceSemaphore:
    """A bit more flexible semaphore than the one found in the standard threading module."""

    def __init__(self):
        self._max_processes = 1
        self._start_process_condition = threading.Condition()
        self._process_count = 0
        self._process_count_lock = threading.Lock()

    def acquire(self, timeout=None):
        """Waits for process count to drop below the limit.

        Args:
            timeout (float, optional): timeout in seconds

        Returns:
            bool: True if semaphore was acquired, False if there were too many processes and a time out occurred
        """
        with self._process_count_lock:
            if self._max_processes == "unlimited":
                self._process_count += 1
                return True
        with self._start_process_condition:
            return self._start_process_condition.wait_for(self._check_and_update_process_count, timeout)

    def __enter__(self):
        return self.acquire()

    def _check_and_update_process_count(self):
        """Atomically checks if process count is less than the limit and if so, increments it by one.

        Returns:
            bool: True if process count was incremented, False otherwise
        """
        with self._process_count_lock:
            if self._process_count < self._max_processes:
                self._process_count += 1
                return True
            return False

    def release(self):
        """Decrements process count and notifies other threads."""
        with self._process_count_lock:
            self._process_count -= 1
            if self._process_count < 0:
                raise RuntimeError("Logic error: process counter negative.")
        with self._start_process_condition:
            self._start_process_condition.notify()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
        return False

    def set_limit(self, limit):
        """Sets maximum number of processes.

        Args:
            limit (int or str): maximum number of processes or "unlimited"
        """
        with self._process_count_lock:
            if limit == self._max_processes:
                return
            previous = self._max_processes
            self._max_processes = limit
        if limit == "unlimited" or (previous != "unlimited" and limit > previous):
            with self._start_process_condition:
                self._start_process_condition.notify_all()


one_shot_process_semaphore = ResourceSemaphore()
persistent_process_semaphore = ResourceSemaphore()
