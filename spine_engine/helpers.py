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


class _AppSettings:
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


class _Message:
    def __init__(self, publisher, event):
        self._publisher = publisher
        self._event = event

    def emit(self, msg):
        self._publisher.dispatch(self._event, msg)


class _PublisherLogger:
    """A :class:`LoggerInterface` compliant logger that uses an EventPublisher.

    When this logger 'emits' messages, it causes the underlying publisher to dispatch events.
    Thus, another logger that suscribes to those events is able to emit the same messages.
    This feels a bit like a hack into our own code, but it may just work.
    """

    def __init__(self, publisher):
        self.msg = _Message(publisher, 'msg')
        self.msg_success = _Message(publisher, 'msg_success')
        self.msg_warning = _Message(publisher, 'msg_warning')
        self.msg_error = _Message(publisher, 'msg_error')
        self.msg_proc = _Message(publisher, 'msg_proc')
        self.msg_proc_error = _Message(publisher, 'msg_proc_error')
