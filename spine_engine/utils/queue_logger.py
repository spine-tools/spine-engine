######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
The QueueLogger class.

:authors: M. Marin (KTH)
:date:   3.11.2020
"""


class _Message:
    def __init__(self, queue, event_type, msg_type, author):
        self._queue = queue
        self._event_type = event_type
        self._msg_type = msg_type
        self._author = author

    def emit(self, msg_text):
        self._queue.put((self._event_type, {"author": self._author, "msg_type": self._msg_type, "msg_text": msg_text}))


class _ExecutionMessage:
    def __init__(self, queue, event_type, author):
        self._queue = queue
        self._event_type = event_type
        self._author = author

    def emit(self, msg):
        self._queue.put((self._event_type, dict(author=self._author, **msg)))


class QueueLogger:
    """A :class:`LoggerInterface` compliant logger that puts messages into a Queue.
    """

    def __init__(self, queue, author):
        self.msg = _Message(queue, "log_msg", "msg", author)
        self.msg_success = _Message(queue, "log_msg", "msg_success", author)
        self.msg_warning = _Message(queue, "log_msg", "msg_warning", author)
        self.msg_error = _Message(queue, "log_msg", "msg_error", author)
        self.msg_proc = _Message(queue, "process_msg", "msg", author)
        self.msg_proc_error = _Message(queue, "process_msg", "msg_error", author)
        self.msg_standard_execution = _ExecutionMessage(queue, "standard_execution_msg", author)
        self.msg_kernel_execution = _ExecutionMessage(queue, "kernel_execution_msg", author)
