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
This class implements a Zero-MQ socket connection received from the ZMQServer.
:author: P. Pääkkönen (VTT)
:date:   19.8.2021
"""

from spine_engine.server.util.server_message import ServerMessage
import json


class ZMQConnection:
    """
    Implementation of a Zero-MQ socket connection.
    Data can be received and sent based on a request/reply pattern enabled by Zero-MQ 
    (see Zero-MQ docs at https://zguide.zeromq.org/).
    """
    def __init__(self, socket, msg_parts):
        """
        Args:
            socket: ZMQSocket for communication
            msg_parts: received message parts
        """
        self._socket = socket
        self._msg_parts = msg_parts

    def get_message_parts(self):
        """Provides Zero-MQ message parts as a list of binary data.

        Returns:
            list: List of binary data.
        """
        return self._msg_parts

    def send_reply(self, data):
        """Sends a reply message to the recipient.

        Args:
            data: Binary data
        """
        self._socket.send(data)

    def send_error_reply(self, cmd, msg_id, msg):
        """Sends an error message to client. Given msg string must be converted
        to JSON str (done by json.dumps() below) or parsing the msg on client
        fails. Do not use \n in msg because it's now allowed in JSON.

        Args:
            cmd (str): Recognized commands are 'execute' or 'ping'
            msg_id (str): Request message id
            msg (str): Error message sent to client
        """
        err_msg_as_json = json.dumps(msg)
        reply_msg = ServerMessage(cmd, msg_id, err_msg_as_json, [])
        reply_as_json = reply_msg.toJSON()
        reply_in_bytes = bytes(reply_as_json, "utf-8")
        self.send_reply(reply_in_bytes)
        print("\nClient has been notified. Moving on...")
