#####################################################################################################################
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
Unit tests for RemotePingHandler class.
:author: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   13.09.2021
"""
import unittest
import zmq
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.connectivity.zmq_server import ZMQServer, ZMQSecurityModelState


class TestRemotePingHandler(unittest.TestCase):

    def setUp(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)

    def tearDown(self):
        if not self.socket.closed:
            self.socket.close()
        if not self.context.closed:
            self.context.term()

    def test_ping_tcp(self):
        """Tests starting of a ZMQ server with tcp, and pinging it."""
        service = ZMQServer("tcp", 5558, ZMQSecurityModelState.NONE, "")
        self.socket.connect("tcp://localhost:5558")
        i = 0
        while i < 10:
            msg_parts = []
            pingMsg = ServerMessage("ping", str(i), "", None)
            pingAsJson = pingMsg.toJSON()
            pingInBytes = bytes(pingAsJson, "utf-8")
            msg_parts.append(pingInBytes)
            self.socket.send_multipart(msg_parts)
            msg = self.socket.recv()
            msgStr = msg.decode("utf-8")
            self.assertEqual(msgStr, pingAsJson)  # check that echoed content is as expected
            i = i + 1
        service.close()

    def test_no_connection(self):
        """Tests pinging a non-existent server."""
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect("tcp://localhost:7002")  # Connect socket somewhere that does not exist
        msg_parts = []
        pingMsg = ServerMessage("ping", "2", "", None)
        pingAsJson = pingMsg.toJSON()
        pingInBytes = bytes(pingAsJson, "utf-8")
        msg_parts.append(pingInBytes)
        sendRet = self.socket.send_multipart(msg_parts, flags=zmq.NOBLOCK)
        event = self.socket.poll(timeout=1000)
        self.assertEqual(0, event)


if __name__ == "__main__":
    unittest.main()
