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
Contains the SpineEngine class for running Spine Toolbox DAGs.

:authors: M. Marin (KTH)
:date:   20.11.2019
"""
import queue
from enum import auto, Enum
from dagster import AssetMaterialization, EventMetadataEntry
from dagster.serdes import whitelist_for_persistence


class ExecutionDirection(Enum):
    FORWARD = auto()
    BACKWARD = auto()

    def __str__(self):
        return {"FORWARD": "forward", "BACKWARD": "backward"}[self.name]


def inverted(input_):
    """Inverts a dictionary of list values.

    Args:
        input_ (dict)

    Returns:
        dict: keys are list items, and values are keys listing that item from the input dictionary
    """
    output = dict()
    for key, value_list in input_.items():
        for value in value_list:
            output.setdefault(value, list()).append(key)
    return output


class _Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class AppSettings:
    """
    A QSettings replacement.
    """

    def __init__(self, settings):
        """
        Init.

        Args:
            settings (dict)
        """
        self._settings = settings

    def value(self, key, defaultValue=""):
        return self._settings.get(key, defaultValue)


class _Signal:
    def __init__(self):
        self._callbacks = set()

    def connect(self, callback):
        self._callbacks.add(callback)

    def disconnect(self, callback):
        self._callbacks.discard(callback)

    def emit(self, msg):
        for callback in self._callbacks:
            callback(msg)


class _LogMessage:
    def __init__(self, type_, text):
        self.type = type_
        self.text = text

    def materialization(self):
        return _LogMessageMaterialization(
            "dummy", metadata_entries=[EventMetadataEntry.text(x, "") for x in (self.type, self.text)]
        )


class _ExecutionMessage:
    def __init__(self, type_, dict_):
        self.type = type_
        self.dict = dict_

    def materialization(self):
        return _ExecutionMessageMaterialization(
            "dummy", metadata_entries=[EventMetadataEntry.text(self.type, ""), EventMetadataEntry.json(self.dict, "")]
        )


class Messenger:
    """A :class:`LoggerInterface` compliant logger that manages a message queue.
    """

    msg = _Signal()
    msg_success = _Signal()
    msg_warning = _Signal()
    msg_error = _Signal()
    msg_proc = _Signal()
    msg_proc_error = _Signal()
    msg_kernel_execution = _Signal()
    msg_standard_execution = _Signal()

    def __init__(self):
        self._msg_queue = queue.Queue()
        self.msg.connect(lambda x: self.put_msg(_LogMessage('msg', x)))
        self.msg_success.connect(lambda x: self.put_msg(_LogMessage('msg_success', x)))
        self.msg_warning.connect(lambda x: self.put_msg(_LogMessage('msg_warning', x)))
        self.msg_error.connect(lambda x: self.put_msg(_LogMessage('msg_error', x)))
        self.msg_proc.connect(lambda x: self.put_msg(_LogMessage('msg_proc', x)))
        self.msg_proc_error.connect(lambda x: self.put_msg(_LogMessage('msg_proc_error', x)))
        self.msg_kernel_execution.connect(lambda x: self.put_msg(_ExecutionMessage('msg_kernel_execution', x)))
        self.msg_standard_execution.connect(lambda x: self.put_msg(_ExecutionMessage('msg_standard_execution', x)))
        self._success = None
        self._current_msg = None
        self._bye_msg = "bye bye"

    def close(self, success):
        """
        Args:
            success (bool)
        """
        self._success = success
        self.put_msg(self._bye_msg)

    def is_closed(self):
        self.update_current_msg()
        if self._current_msg == self._bye_msg:
            self.task_done()
            return True
        return False

    def success(self):
        return self._success

    def update_current_msg(self):
        if self._current_msg is None:
            self._current_msg = self._msg_queue.get()

    def get_msg(self):
        self.update_current_msg()
        return self._current_msg

    def put_msg(self, msg):
        self._msg_queue.put(msg)

    def task_done(self):
        self._current_msg = None
        self._msg_queue.task_done()


@whitelist_for_persistence
class ItemExecutionStart(AssetMaterialization):
    def dispatch_event(self, engine):
        item_name = self.metadata_entries[0].entry_data.text
        engine._publisher.dispatch('exec_started', {"item_name": item_name, "direction": ExecutionDirection.FORWARD})


@whitelist_for_persistence
class ItemExecutionFinish(AssetMaterialization):
    def dispatch_event(self, engine):
        item_name = self.metadata_entries[0].entry_data.text
        engine._publisher.dispatch(
            'exec_finished', {"item_name": item_name, "direction": ExecutionDirection.FORWARD, "state": engine.state()}
        )


@whitelist_for_persistence
class _LogMessageMaterialization(AssetMaterialization):
    def dispatch_event(self, engine):
        msg_type, msg_text = [x.entry_data.text for x in self.metadata_entries]
        engine._publisher.dispatch("msg", {"msg_type": msg_type, "msg_text": msg_text})


@whitelist_for_persistence
class _ExecutionMessageMaterialization(AssetMaterialization):
    def dispatch_event(self, engine):
        msg_type = self.metadata_entries[0].entry_data.text
        msg_dict = self.metadata_entries[1].entry_data.data
        engine._publisher.dispatch(msg_type, msg_dict)
