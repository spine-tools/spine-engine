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
The LoggingProcess class.

:authors: M. Marin (KTH)
:date:   3.11.2020
"""

import multiprocessing as mp


class _Signal:
    """A PySide2.QtCore.Signal replacement.
    """

    def __init__(self):
        self._callbacks = set()

    def connect(self, callback):
        self._callbacks.add(callback)

    def disconnect(self, callback):
        self._callbacks.discard(callback)

    def emit(self, msg):
        for callback in self._callbacks:
            callback(msg)


class _QueueLogger:
    """A Logger that puts the messages into a queue.
    """

    msg = _Signal()
    msg_success = _Signal()
    msg_warning = _Signal()
    msg_error = _Signal()
    msg_proc = _Signal()
    msg_proc_error = _Signal()

    def __init__(self, queue):
        """
        Args:
            queue (multiprocessing.Queue)
        """
        self._queue = queue
        self.msg.connect(lambda x: self._put_msg(('msg', x)))
        self.msg_success.connect(lambda x: self._put_msg(('msg_success', x)))
        self.msg_warning.connect(lambda x: self._put_msg(('msg_warning', x)))
        self.msg_error.connect(lambda x: self._put_msg(('msg_error', x)))
        self.msg_proc.connect(lambda x: self._put_msg(('msg_proc', x)))
        self.msg_proc_error.connect(lambda x: self._put_msg(('msg_proc_error', x)))

    def _put_msg(self, msg):
        self._queue.put(msg)


class LoggingProcess(mp.Process):
    _JOB_DONE = "job_done"

    def __init__(self, logger, *args, **kwargs):
        """

        Args:
            logger (LoggerInterface): A logger to forward messages to
        """
        super().__init__(*args, **kwargs)
        self._logger = logger
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
        while True:
            msg_type, msg_content = self._queue.get()
            if msg_type == self._JOB_DONE:
                self._success = msg_content
                break
            getattr(self._logger, msg_type).emit(msg_content)
        self.join()

    def run(self):
        self._initialize_logging()
        if self._target:
            success = self._target(*self._args, **self._kwargs)
        else:
            success = None
        self._queue.put((self._JOB_DONE, success))

    def terminate(self):
        super().terminate()
        self._queue.put((self._JOB_DONE, False))

    def _initialize_logging(self):
        """Initializes logging in this process.
        (This must be called once inside ``run()`` to work as intended.)
        After calling this, calls to ``get_logger()`` inside the target function
        will return a dedicated ``_QueueLogger`` for this process.
        """
        _queue_logger.append(_QueueLogger(self._queue))


_queue_logger = []


def get_logger():
    return _queue_logger[0]
