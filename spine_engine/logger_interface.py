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
"""A logger interface for static type checking."""
from typing import Protocol


class MessageSignal(Protocol):
    def emit(self, message: str):
        pass


class MessageBoxSignal(Protocol):
    def emit(self, title: str, message: str):
        pass


class LoggerInterface(Protocol):
    """Protocol for logger that uses signals that can be emitted to send messages to an output device."""

    msg: MessageSignal
    """Emits a notification message."""
    msg_success: MessageSignal
    """Emits a message on success"""
    msg_warning: MessageSignal
    """Emits a warning message."""
    msg_error: MessageSignal
    """Emits an error message."""
    msg_proc: MessageSignal
    """Emits a message originating from a subprocess (usually something printed to stdout)."""
    msg_proc_error: MessageSignal
    """Emits an error message originating from a subprocess (usually something printed to stderr)."""
    information_box: MessageBoxSignal
    """Requests an 'information message box' (e.g. a message window) to be opened with a given title and message."""
    error_box: MessageBoxSignal
    """Requests an 'error message box' to be opened with a given title and message."""
