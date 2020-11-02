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
Helpers functions and classes.

:authors: M. Marin (KTH)
:date:   20.11.2019
"""
from enum import auto, Enum


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


class _LogMessage:
    def __init__(self, publisher, msg_type):
        self._publisher = publisher
        self._msg_type = msg_type

    def emit(self, msg_text):
        self._publisher.dispatch("msg", {'msg_type': self._msg_type, 'msg_text': msg_text})


class _ExecutionMessage:
    def __init__(self, publisher, event_type):
        self._publisher = publisher
        self._event_type = event_type

    def emit(self, msg):
        self._publisher.dispatch(self._event_type, msg)


class PublisherLogger:
    """A :class:`LoggerInterface` compliant logger that uses an EventPublisher.

    When this logger 'emits' messages, it causes the underlying publisher to dispatch events.
    Thus, another logger that suscribes to those events is able to emit the same messages.
    This feels a bit like a hack into our own code, but it may just work.
    """

    def __init__(self, publisher):
        self.publisher = publisher
        self.msg = _LogMessage(publisher, 'msg')
        self.msg_success = _LogMessage(publisher, 'msg_success')
        self.msg_warning = _LogMessage(publisher, 'msg_warning')
        self.msg_error = _LogMessage(publisher, 'msg_error')
        self.msg_proc = _LogMessage(publisher, 'msg_proc')
        self.msg_proc_error = _LogMessage(publisher, 'msg_proc_error')
        self.msg_standard_execution = _ExecutionMessage(publisher, 'msg_standard_execution')
        self.msg_kernel_execution = _ExecutionMessage(publisher, 'msg_kernel_execution')
