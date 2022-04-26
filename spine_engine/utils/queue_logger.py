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
The QueueLogger class.

:authors: M. Marin (KTH)
:date:   3.11.2020
"""


class _Message:
    def __init__(self, queue, event_type, msg_type, item_name):
        self._queue = queue
        self._event_type = event_type
        self._msg_type = msg_type
        self._item_name = item_name
        self._filter_id = ""

    @property
    def filter_id(self):
        return self._filter_id

    @filter_id.setter
    def filter_id(self, filter_id):
        self._filter_id = filter_id

    def emit(self, msg_text):
        self._queue.put(
            (
                self._event_type,
                {
                    "item_name": self._item_name,
                    "filter_id": self._filter_id,
                    "msg_type": self._msg_type,
                    "msg_text": msg_text,
                },
            )
        )


class _Flash:
    def __init__(self, queue, item_name):
        self._queue = queue
        self._item_name = item_name

    def emit(self):
        self._queue.put(("flash", {"item_name": self._item_name}))


class _Prompt:
    def __init__(self, queue, item_name, prompt_queue, answered_prompts):
        self._queue = queue
        self._item_name = item_name
        self._prompt_queue = prompt_queue
        self._answered_prompts = answered_prompts
        self._filter_id = ""

    @property
    def filter_id(self):
        return self._filter_id

    @filter_id.setter
    def filter_id(self, filter_id):
        self._filter_id = filter_id

    def emit(self, prompt):
        prompt = {"item_name": self._item_name, **prompt}
        key = str(prompt)
        if key not in self._answered_prompts:
            self._queue.put(("prompt", prompt))
            self._answered_prompts[key] = self._prompt_queue.get()
        return self._answered_prompts[key]


class _ExecutionMessage:
    def __init__(self, queue, event_type, item_name):
        self._queue = queue
        self._event_type = event_type
        self._item_name = item_name
        self._filter_id = ""

    @property
    def filter_id(self):
        return self._filter_id

    @filter_id.setter
    def filter_id(self, filter_id):
        self._filter_id = filter_id

    def emit(self, msg):
        self._queue.put((self._event_type, dict(item_name=self._item_name, filter_id=self._filter_id, **msg)))


class SuppressedMessage:
    def __init__(self, *args, **kwargs):
        pass

    def emit(self, *args, **kwargs):
        """Don't emit anything"""


class QueueLogger:
    """A :class:`LoggerInterface` compliant logger that puts messages into a Queue."""

    def __init__(self, queue, item_name, prompt_queue, answered_prompts, silent=False):
        self._silent = silent
        message = _Message if not silent else SuppressedMessage
        execution_message = _ExecutionMessage if not silent else SuppressedMessage
        self.flash = _Flash(queue, item_name)
        self.msg = message(queue, "event_msg", "msg", item_name)
        self.msg_success = message(queue, "event_msg", "msg_success", item_name)
        self.msg_warning = message(queue, "event_msg", "msg_warning", item_name)
        self.msg_error = message(queue, "event_msg", "msg_error", item_name)
        self.msg_proc = message(queue, "process_msg", "msg", item_name)
        self.msg_proc_error = message(queue, "process_msg", "msg_error", item_name)
        self.msg_standard_execution = execution_message(queue, "standard_execution_msg", item_name)
        self.msg_persistent_execution = execution_message(queue, "persistent_execution_msg", item_name)
        self.msg_kernel_execution = execution_message(queue, "kernel_execution_msg", item_name)
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
