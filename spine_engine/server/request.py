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
Contains a class for client requests received at server.
"""

import json
from spine_engine.server.util.server_message import ServerMessage


class Request:
    """Class for bundling the received request and associated data together."""

    def __init__(self, msg, cmd, request_id, data, filenames):
        """Init class.

        Args:
            msg (list): List of three or four binary frames (conn id, empty frame and user data frame,
                and possibly zip-file)
            cmd (str): Command associated with the request
            request_id (str): Client request Id
            data (bytes): Zip-file
            filenames (list): List of associated filenames
        """
        self._msg = msg
        self._cmd = cmd
        self._request_id = request_id
        self._data = data
        self._filenames = filenames
        self._connection_id = msg[0]  # Assigned by the frontend (ROUTER) socket that received the message
        self._zip_file = None
        if len(msg) == 3:
            self._zip_file = msg[2]

    def msg(self):
        """Returns a list containing three binary frames.
        First frame is the connection id (added by the frontend ROUTER socket at receiver).
        The second frame is empty (added by the frontend ROUTER socket at receiver).
        The third frame contains the data sent by client."""
        return self._msg

    def cmd(self):
        """Returns the command as string (eg. 'execute' or 'ping' associated to this request)."""
        return self._cmd

    def request_id(self):
        """Returns the request id as string of the received ServerMessage.
        Assigned by client when the request was made."""
        return self._request_id

    def data(self):
        """Returns the parsed msg associated to this request."""
        return self._data

    def filenames(self):
        """Returns associated filenames if any."""
        return self._filenames

    def connection_id(self):
        """Returns the connection Id as binary string. Assigned by the frontend
        ROUTER socket when the message was received at server."""
        return self._connection_id

    def zip_file(self):
        """Returns the binary zip file (zipped project dir) associated with the message
        or None if the message did not contain a zip-file."""
        return self._zip_file

    def send_response(self, socket, info, internal_msg=None, dump_to_json=True):
        """Sends a response message to client. Do not use \n in the internal_msg (not allowed in JSON).

        Args:
            socket (ZMQSocket): Socket for sending the reply
            info (tuple): Message (data) tuple for client [event_type, msg]
            internal_msg (tuple): Internal server message, [job_id, msg]
            dump_to_json (bool): If True, info is dumped to a JSON str. When False, info must be a JSON str already.
        """
        info_as_json = json.dumps(info) if dump_to_json else info
        reply_msg = ServerMessage(self._cmd, self._request_id, info_as_json, [])
        if not internal_msg:
            self.send_multipart_reply(socket, self.connection_id(), reply_msg.to_bytes())
        else:
            internal_msg_json = json.dumps(internal_msg)
            self.send_multipart_reply(socket, self.connection_id(), reply_msg.to_bytes(), internal_msg_json)

    @staticmethod
    def send_multipart_reply(socket, connection_id, data, internal_msg=None):
        """Sends a multi-part (multi-frame) response.

        Args:
            socket (ZMQSocket): Socket for sending the reply
            connection_id (bytes): Client Id. Assigned by the frontend ROUTER socket when a request is received.
            data (bytes): User data to be sent
            internal_msg (str): Internal server message as JSON string
        """
        if not internal_msg:
            frame = [connection_id, b"", data]
        else:
            frame = [connection_id, b"", data, internal_msg.encode("utf-8")]
        socket.send_multipart(frame)

    @staticmethod
    def send_multipart_reply_with_file(socket, connection_id, data, file, internal_msg):
        """Sends a multi-part (multi-frame) response.

        Args:
            socket (ZMQSocket): Socket for sending the reply
            connection_id (bytes): Client Id. Assigned by the frontend ROUTER socket when a request is received.
            data (bytes): User data to be sent
            file (bytes): File to transmit to client
            internal_msg (str): Internal server message as JSON string
        """
        frame = [connection_id, b"", data, file, internal_msg.encode("utf-8")]
        socket.send_multipart(frame)
