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

import json
from spine_engine.server.util.server_message import ServerMessage


class ZMQConnection:
    """Implementation of a Zero-MQ socket connection."""
    def __init__(self, msg, socket, cmd, rqst_id, data, filenames):
        """Init class.

        Args:
            msg (list): List of three or four binary frames (conn id, empty frame and user data frame,
                and possibly zip-file)
            socket (ZMQSocket): Socket connected to the received message
            cmd (str): Command associated with the request
            rqst_id (str): Request id (assigned by ZMQ)
            data (bytes): Zip-file
            filenames (list): List of associated filenames
        """
        self._msg = msg
        self._socket = socket
        self._cmd = cmd
        self._request_id = rqst_id
        self._data = data
        self._filenames = filenames
        self._connection_id = msg[0]  # Assigned by the ROUTER socket that received the message
        self._zip_file = None
        if len(msg) == 4:
            self._zip_file = msg[3]

    def msg(self):
        """Returns a list containing three binary frames.
         First frame is the connection id (added by the frontend ROUTER socket at receiver).
         The second frame is empty (added by the frontend ROUTER socket at receiver).
         The third frame contains the data sent by client."""
        return self._msg

    def socket(self):
        """Returns the socket associated to this connection."""
        return self._socket

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
        """Returns the connection Id as binary string. Assigned by the ROUTER
        socket when the message was received at server."""
        return self._connection_id

    def zip_file(self):
        """Returns the binary zip file (zipped project dir) associated with the message
        or None if the message did not contain a zip-file."""
        return self._zip_file

    def send_reply(self, data):
        """Sends a one-part reply message to the recipient.
        Does not work with ROUTER sockets.

        Args:
            data (bytes): Binary string
        """
        self._socket.send(data)

    def send_multipart_reply(self, data):
        """Sends a multi-part (multi-frame) response.
        Responding to clients from ROUTER sockets must use this method.

        Args:
            data (bytes): User data to be sent
        """
        frame = [self._connection_id, b"", data]
        self._socket.send_multipart(frame)

    def send_response(self, response_data):
        """Sends reply back to client. Used after execution to send the events to client."""
        reply_msg = ServerMessage(self._cmd, self._request_id, response_data, None)
        reply_as_json = reply_msg.toJSON()
        reply_in_bytes = bytes(reply_as_json, "utf-8")
        self.send_multipart_reply(reply_in_bytes)

    def send_error_reply(self, response_msg):
        """Sends an error message to client. Given msg string must be converted
        to JSON str (done by json.dumps() below) or parsing the msg on client
        fails. Do not use \n in the reply because it's not allowed in JSON.

        Args:
            response_msg (str): Error message sent to client
        """
        print("send_error_reply")
        err_msg_as_json = json.dumps(response_msg)
        reply_msg = ServerMessage(self._cmd, self._request_id, err_msg_as_json, [])
        reply_as_json = reply_msg.toJSON()
        reply_in_bytes = bytes(reply_as_json, "utf-8")
        self.send_multipart_reply(reply_in_bytes)
        print("\nClient has been notified. Moving on...")

    @staticmethod
    def send_init_failed_reply(socket, response_msg):
        """Sends an error reply to client when there was something
        wrong in the received message."""
        print("send_init_failed_reply")
        err_msg_as_json = json.dumps(response_msg)
        reply_msg = ServerMessage("", "", err_msg_as_json, [])
        reply_as_json = reply_msg.toJSON()
        reply_in_bytes = bytes(reply_as_json, "utf-8")
        socket.send_multipart_reply(reply_in_bytes)
        print("\nClient has been notified. Moving on...")
