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
Contains a class for handling ping requests.
"""

import json
import threading
import zmq  # pylint: disable=unused-import
from spine_engine.server.service_base import ServiceBase
from spine_engine.server.util.server_message import ServerMessage


class PingService(threading.Thread, ServiceBase):
    """Class for handling ping requests."""

    def __init__(self, context, request, job_id):
        """Initializes instance.

        Args:
            context (zmq.Context): Server context
            request (Request): Client request
            job_id (str): Worker thread Id
        """
        super(PingService, self).__init__(name="PingServiceThread")
        ServiceBase.__init__(self, context, request, job_id)

    def run(self):
        """Replies to a ping command."""
        self.worker_socket.connect("inproc://backend")
        reply_msg = ServerMessage("ping", self.request.request_id(), "", None)
        internal_msg = json.dumps((self.job_id, "completed"))
        conn_id = self.request.connection_id()
        rep = reply_msg.to_bytes()
        self.request.send_multipart_reply(self.worker_socket, conn_id, rep, internal_msg)
