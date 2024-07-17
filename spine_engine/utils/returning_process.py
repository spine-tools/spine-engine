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

"""
The ReturningProcess class.

"""

from contextlib import contextmanager
from enum import Enum, auto, unique
import multiprocessing as mp
import threading
from .execution_resources import one_shot_process_semaphore


class ReturningProcess(mp.Process):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue = mp.Queue()
        self._terminated = False
        self.maybe_idle = _MaybeIdle()  # target functions should enter this context whenever they may become idle

    def run_until_complete(self):
        """Starts the process and joins it after it has finished.

        Returns:
            tuple: Return value of the process where the first element is a status flag
        """
        with one_shot_process_semaphore:
            if self._terminated:
                return (False,)
            with self.maybe_idle.listen():
                self.start()
                return_value = self._queue.get()
                self.join()
            return return_value

    def run(self):
        if not self._target:
            self._queue.put((False,))
            return
        return_value = self._target(self, *self._args, **self._kwargs)
        self._queue.put(return_value)

    def terminate(self):
        self._terminated = True
        if self.is_alive():
            super().terminate()
        self._queue.put((False,))


class _MaybeIdle:
    """A context manager to temporarily release the one shot process semaphore.
    This allows us to prevent deadlocks when processes need to wait for others to do certain actions in order to
    proceed (e.g., when executable items need to write to a DB in a certain order and we can't predict in which order
    their processes will start).
    """

    @unique
    class _Event(Enum):
        ENTER = auto()
        EXIT = auto()

    def __init__(self):
        self._queue = mp.Queue()

    def __enter__(self):
        self._queue.put(self._Event.ENTER)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._queue.put(self._Event.EXIT)

    @contextmanager
    def listen(self):
        thread = threading.Thread(target=self._do_listen)
        thread.start()
        try:
            yield
        finally:
            self._queue.put(None)
            thread.join()

    def _do_listen(self):
        while True:
            event = self._queue.get()
            if event == self._Event.ENTER:
                one_shot_process_semaphore.release()
            elif event == self._Event.EXIT:
                one_shot_process_semaphore.acquire()
            elif event is None:
                break


if __name__ == "__main__":
    # https://docs.python.org/3.7/library/multiprocessing.html?highlight=freeze_support#multiprocessing.freeze_support
    mp.freeze_support()
