######################################################################################################################
# Copyright (C) 2017-2020 Spine project consortium
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
:date:   3.11.2020
"""

import multiprocessing as mp


class ReturningProcess(mp.Process):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue = mp.Queue()
        self._success = None

    @property
    def success(self):
        return self._success

    def run_until_complete(self):
        """Starts the process, forwards all the messages to the logger,
        and finally joins the process.
        """
        self.start()
        self._success = self._queue.get()
        self.join()

    def run(self):
        if self._target:
            success = self._target(*self._args, **self._kwargs)
        else:
            success = None
        self._queue.put(success)

    def terminate(self):
        super().terminate()
        self._queue.put(False)
