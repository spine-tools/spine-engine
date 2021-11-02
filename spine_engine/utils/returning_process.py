######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
The ReturningProcess class.

:authors: M. Marin (KTH)
:date:    3.11.2020
"""

from contextlib import contextmanager
import multiprocessing as mp
import threading


class ReturningProcess(mp.Process):
    _MAX_PROCESSES = 1
    _start_process_condition = threading.Condition()
    _process_count = 0
    _process_count_lock = threading.Lock()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue = mp.Queue()
        self._terminated = False

    def run_until_complete(self):
        """Starts the process and joins it after it has finished.

        Returns:
            tuple: Return value of the process where the first element is a status flag
        """
        with acquire_resources(ReturningProcess):
            if self._terminated:
                return (False,)
            self.start()
            return_value = self._queue.get()
            self.join()
            return return_value

    @classmethod
    def acquire_process(cls):
        """Waits for process count to drop below _MAX_PROCESSES."""
        with cls._start_process_condition:
            cls._start_process_condition.wait_for(cls._check_and_update_process_count)

    @classmethod
    def _check_and_update_process_count(cls):
        """Atomically checks if process count is less than _MAX_PROCESSES and if so, increments it by one.

        Returns:
            bool: True if process count was incremented, False otherwise
        """
        with cls._process_count_lock:
            if cls._process_count < cls._MAX_PROCESSES:
                cls._process_count += 1
                return True
            else:
                return False

    @classmethod
    def release_process(cls):
        """Decrements process count and notifies other threads."""
        with cls._process_count_lock:
            cls._process_count -= 1
            if cls._process_count < 0:
                raise RuntimeError("Logic error: engine process counter negative.")
        with cls._start_process_condition:
            cls._start_process_condition.notify()

    def run(self):
        if not self._target:
            self._queue.put((False,))
        result = self._target(*self._args, **self._kwargs)
        self._queue.put(result)

    def terminate(self):
        self._terminated = True
        if self.is_alive():
            super().terminate()
        self._queue.put((False,))


@contextmanager
def acquire_resources(cls):
    try:
        cls.acquire_process()
        yield None
    finally:
        cls.release_process()


if __name__ == "__main__":
    # https://docs.python.org/3.7/library/multiprocessing.html?highlight=freeze_support#multiprocessing.freeze_support
    mp.freeze_support()
