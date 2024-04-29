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
Contains the ExecutionManagerBase class.

"""


class ExecutionManagerBase:
    """Base class for all tool instance execution managers."""

    def __init__(self, logger):
        """Class constructor.

        Args:
            logger (LoggerInterface): a logger instance
        """
        self._logger = logger
        self.killed = False

    def run_until_complete(self):
        """Runs until completion.

        Returns:
            int: return code
        """
        raise NotImplementedError()

    def stop_execution(self):
        """Stops execution gracefully."""
        raise NotImplementedError()
