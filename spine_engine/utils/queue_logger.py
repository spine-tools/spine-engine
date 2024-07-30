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
The QueueLogger class.

"""
import time


class _MessageBase:
    def __init__(self, queue, item_name, event_type):
        self._queue = queue
        self._event_type = event_type
        self._item_name = item_name
        self._filter_id = ""
        self._slots = []

    @property
    def filter_id(self):
        return self._filter_id

    @filter_id.setter
    def filter_id(self, filter_id):
        self._filter_id = filter_id

    def emit(self, msg):
        msg = dict(filter_id=self._filter_id, **msg)
        full_msg = (self._event_type, dict(item_name=self._item_name, **msg))
        self._queue.put(full_msg)
        for slot in self._slots:
            slot(msg)

    def connect(self, slot):
        self._slots.append(slot)

    def disconnect(self, slot):
        self._slots.remove(slot)


class _Message(_MessageBase):
    def __init__(self, queue, item_name, event_type, msg_type):
        super().__init__(queue, item_name, event_type)
        self._msg_type = msg_type

    def emit(self, msg_text):
        super().emit({"msg_type": self._msg_type, "msg_text": msg_text})


class _ExecutionMessage(_MessageBase):
    pass


class SuppressedMessage:
    def __init__(self, *args, **kwargs):
        pass

    def emit(self, *args, **kwargs):
        """Don't emit anything"""

    def connect(self, *args, **kwargs):
        """Don't connect anything"""


class _Prompt(_MessageBase):
    _PENDING = object()

    def __init__(self, queue, item_name, prompt_queue, answered_prompts):
        super().__init__(queue, item_name, "prompt")
        self._prompt_queue = prompt_queue
        self._answered_prompts = answered_prompts

    def emit(self, prompt_data):
        key = str(prompt_data)
        if key not in self._answered_prompts:
            self._answered_prompts[key] = self._PENDING
            prompt = {"prompter_id": id(self._prompt_queue), "data": prompt_data}
            self._queue.put(("prompt", prompt))
            self._answered_prompts[key] = self._prompt_queue.get()
        while self._answered_prompts[key] is self._PENDING:
            time.sleep(0.02)
        return self._answered_prompts[key]


class _Flash(_MessageBase):
    def __init__(self, queue, item_name):
        super().__init__(queue, item_name, "flash")

    def emit(self):
        self._queue.put(("flash", {"item_name": self._item_name}))


class QueueLogger:
    """A :class:`LoggerInterface` compliant logger that puts messages into a Queue."""

    def __init__(self, queue, item_name, prompt_queue, answered_prompts, silent=False):
        self._silent = silent
        message = _Message if not silent else SuppressedMessage
        execution_message = _ExecutionMessage if not silent else SuppressedMessage
        self.flash = _Flash(queue, item_name)
        self.msg = message(queue, item_name, "event_msg", "msg")
        self.msg_success = message(queue, item_name, "event_msg", "msg_success")
        self.msg_warning = message(queue, item_name, "event_msg", "msg_warning")
        self.msg_error = message(queue, item_name, "event_msg", "msg_error")
        self.msg_proc = message(queue, item_name, "process_msg", "msg")
        self.msg_proc_error = message(queue, item_name, "process_msg", "msg_error")
        self.msg_standard_execution = execution_message(queue, item_name, "standard_execution_msg")
        self.msg_persistent_execution = execution_message(queue, item_name, "persistent_execution_msg")
        self.msg_kernel_execution = execution_message(queue, item_name, "kernel_execution_msg")
        self.prompt = _Prompt(queue, item_name, prompt_queue, answered_prompts)

    def set_filter_id(self, filter_id):
        if not self._silent:
            self.msg.filter_id = filter_id
            self.msg_success.filter_id = filter_id
            self.msg_warning.filter_id = filter_id
            self.msg_error.filter_id = filter_id
            self.msg_proc.filter_id = filter_id
            self.msg_proc_error.filter_id = filter_id
            self.msg_standard_execution.filter_id = filter_id
            self.msg_persistent_execution.filter_id = filter_id
            self.msg_kernel_execution.filter_id = filter_id
        self.prompt.filter_id = filter_id
