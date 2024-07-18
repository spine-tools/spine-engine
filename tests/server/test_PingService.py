#####################################################################################################################
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
Unit tests for PingService class.
"""

import unittest
import zmq
from spine_engine.server.engine_server import EngineServer, ServerSecurityModel
from spine_engine.server.util.server_message import ServerMessage


class TestPingService(unittest.TestCase):
    def setUp(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)

    def tearDown(self):
        if not self.socket.closed:
            self.socket.close()
        if not self.context.closed:
            self.context.term()

    def test_ping_tcp(self):
        service = EngineServer("tcp", 5558, ServerSecurityModel.NONE, "")
        self.socket.connect("tcp://localhost:5558")
        i = 0
        while i < 10:
            ping_msg = ServerMessage("ping", str(i), "", None)
            self.socket.send_multipart([ping_msg.to_bytes()])
            response = self.socket.recv_multipart()
            response_str = response[1].decode("utf-8")
            ping_as_json = ping_msg.toJSON()
            self.assertEqual(response_str, ping_as_json)  # Check that echoed content is as expected
            i = i + 1
        service.close()

    def test_no_connection(self):
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect("tcp://localhost:7002")  # Connect socket somewhere that does not exist
        msg_parts = []
        ping_msg = ServerMessage("ping", "2", "", None)
        msg_parts.append(ping_msg.to_bytes())
        self.socket.send_multipart(msg_parts, flags=zmq.NOBLOCK)
        event = self.socket.poll(timeout=1000)
        self.assertEqual(0, event)


if __name__ == "__main__":
    unittest.main()
