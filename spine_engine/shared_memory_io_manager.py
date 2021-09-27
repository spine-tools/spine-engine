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
Contains Engine's IO manager.

:authors: A. Soininen (VTT)
:date:    6.7.2021
"""
from dagster import IOManager, io_manager


@io_manager
def shared_memory_io_manager(init_context):
    return SharedMemoryIOManager()


class SharedMemoryIOManager(IOManager):
    """An IO manager that stores values in shared storage."""

    _shared_values = dict()

    def handle_output(self, context, obj):
        """Stores values in memory."""
        keys = tuple(context.get_output_identifier())
        self._shared_values[keys] = obj

    def load_input(self, context):
        """Loads value from memory."""
        keys = tuple(context.upstream_output.get_output_identifier())
        return self._shared_values[keys]
