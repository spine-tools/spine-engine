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
Contains a base class for different services provided by the Spine Engine Server.
"""

import zmq


class ServiceBase:
    """Service base class."""

    def __init__(self, context, request, job_id):
        """Initializes instance.

        Args:
            context (zmq.Context): Context for this handler.
            request (Request): Client request
            job_id (str): Worker thread Id
        """
        self.context = context
        self.request = request
        self.job_id = job_id
        self.worker_socket = self.context.socket(zmq.DEALER)
        self.n_port_range = 50  # Port range for push and pub ports

    def close(self):
        """Closes socket after thread has finished."""
        self.worker_socket.close()
