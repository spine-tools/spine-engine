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

import multiprocessing as mp
from .execution_resources import one_shot_process_semaphore


class ReturningProcess(mp.Process):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue = mp.Queue()
        self._terminated = False

    def run_until_complete(self):
        """Starts the process and joins it after it has finished.

        Returns:
            tuple: Return value of the process where the first element is a status flag
        """
        with one_shot_process_semaphore:
            if self._terminated:
                return (False,)
            self.start()
            return_value = self._queue.get()
            self.join()
            return return_value

    def run(self):
        if not self._target:
            self._queue.put((False,))
            return
        result = self._target(*self._args, **self._kwargs)
        self._queue.put(result)

    def terminate(self):
        self._terminated = True
        if self.is_alive():
            super().terminate()
        self._queue.put((False,))


if __name__ == "__main__":
    # https://docs.python.org/3.7/library/multiprocessing.html?highlight=freeze_support#multiprocessing.freeze_support
    mp.freeze_support()
