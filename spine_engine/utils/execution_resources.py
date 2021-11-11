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
Utilities for managing execution resources such as processes.

:authors: A. Soininen (VTT)
:date:    9.11.2021
"""
import threading


class OneShotProcessSemaphore:
    """Contains a class attribute semaphore that acts as limiter for simultaneous 'one-shot' processes."""

    semaphore = threading.BoundedSemaphore(1)


class PersistentProcessSemaphore:
    """Contains a class attribute semaphore that acts as limiter for simultaneous persistent processes."""

    semaphore = threading.BoundedSemaphore(1)


class NonSemaphore:
    """A mock semaphore that can always be acquired."""

    @staticmethod
    def acquire(blocking=False, timeout=None):
        return True

    @staticmethod
    def release(self):
        return
