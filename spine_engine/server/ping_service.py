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
Contains a class for handling ping requests.
:authors: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   13.09.2021
"""

import threading
import json
import zmq
from spine_engine.server.util.server_message import ServerMessage


class PingService(threading.Thread):
    """Class for handling ping requests."""
    def __init__(self, context, request, job_id):
        """Initializes instance.

        Args:
            context (zmq.Context): Server context
            request (Request): Client request
            job_id (str): Worker thread Id
        """
        super().__init__(name="PingHandlerThread")
        self.context = context
        self.req = request
        self.job_id = job_id
        self.ping_socket = self.context.socket(zmq.DEALER)  # Backend socket

    def run(self):
        """Replies to a ping command."""
        self.ping_socket.connect("inproc://backend")
        reply_msg = ServerMessage("ping", self.req.request_id(), "", None)
        internal_msg = json.dumps((self.job_id, ""))
        self.req.send_multipart_reply(self.ping_socket, self.req.connection_id(), reply_msg.to_bytes(), internal_msg)

    def close(self):
        """Closes socket and cleans up."""
        self.ping_socket.close()
