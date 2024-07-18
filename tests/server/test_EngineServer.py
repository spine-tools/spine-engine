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

""" Unit tests for EngineServer class. """

import os
import pathlib
import threading
import unittest
import zmq
from spine_engine.server.engine_server import EngineServer, ServerSecurityModel
from spine_engine.server.util.server_message import ServerMessage

base_dir = os.path.join(str(pathlib.Path(__file__).parent), "secfolder_for_tests")


def _security_folder_exists():
    """Security folder and allowEndpoints.txt must exist to test security."""
    endpointfile = os.path.join(base_dir, "allowEndpoints.txt")
    return os.path.exists(base_dir) and os.path.exists(endpointfile)


class TestEngineServer(unittest.TestCase):
    def setUp(self):
        """Sets up client context and socket."""
        self.client_context = zmq.Context()
        self.req_socket = self.client_context.socket(zmq.DEALER)
        self.base_dir = base_dir

    def tearDown(self):
        """Closes client socket and context if open."""
        if not self.req_socket.closed:
            self.req_socket.close()
        if not self.client_context.closed:
            self.client_context.term()

    def test_missing_secfolder(self):
        """Tests the starting/stopping of the ZMQ server without proper sec folder"""
        server = None
        with self.assertRaises(ValueError):
            server = EngineServer("tcp", 6002, ServerSecurityModel.STONEHOUSE, "")
        self.assertIsNone(server)

    def test_invalid_secfolder(self):
        """Tests the starting the ZMQ server without a proper sec folder."""
        server = None
        with self.assertRaises(ValueError):
            server = EngineServer("tcp", 6003, ServerSecurityModel.STONEHOUSE, "/fwhkjfnsefkjnselk")
        self.assertIsNone(server)

    def test_starting_server(self):
        """Tests starting a tcp ZMQ server without security and pinging it."""
        server = EngineServer("tcp", 5556, ServerSecurityModel.NONE, "")
        self.req_socket.connect("tcp://localhost:5556")
        # Ping the server
        ping_msg = ServerMessage("ping", "123", "", None)
        self.req_socket.send_multipart([ping_msg.to_bytes()])
        response = self.req_socket.recv_multipart()
        response_str = response[1].decode("utf-8")
        ping_as_json = ping_msg.toJSON()
        self.assertEqual(response_str, ping_as_json)  # check that echoed content is as expected
        server.close()

    @unittest.skipIf(not _security_folder_exists(), "Test requires a security folder")
    def test_starting_server_with_security(self):
        """Tests starting a tcp ZMQ server with StoneHouse Security and pinging it."""
        server = EngineServer("tcp", 6006, ServerSecurityModel.STONEHOUSE, base_dir)
        # Configure client security
        secret_keys_dir = os.path.join(base_dir, "private_keys")
        keys_dir = os.path.join(base_dir, "certificates")
        public_keys_dir = os.path.join(base_dir, "public_keys")
        # We need two certificates, one for the client and one for the server.
        client_secret_file = os.path.join(secret_keys_dir, "client.key_secret")
        client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
        self.req_socket.curve_secretkey = client_secret
        self.req_socket.curve_publickey = client_public
        server_public_file = os.path.join(public_keys_dir, "server.key")
        server_public, _ = zmq.auth.load_certificate(server_public_file)
        # The client must know the server's public key to make a CURVE connection.
        self.req_socket.curve_serverkey = server_public
        self.req_socket.connect("tcp://localhost:6006")
        # Ping the server
        ping_msg = ServerMessage("ping", "123", "", None)
        self.req_socket.send_multipart([ping_msg.to_bytes()])
        response = self.req_socket.recv_multipart()
        response_str = response[1].decode("utf-8")
        ping_as_json = ping_msg.toJSON()
        self.assertEqual(response_str, ping_as_json)  # check that echoed content is as expected
        server.close()

    def test_malformed_server_message(self):
        """Tests what happens when the sent request is not valid."""
        server = EngineServer("tcp", 5556, ServerSecurityModel.NONE, "")
        self.req_socket.connect("tcp://localhost:5556")
        file_like_object = bytearray([1, 2, 3, 4, 5])
        req = b"feiofnoknfsdnoiknsmd"
        self.req_socket.send_multipart([req, file_like_object])
        response = self.req_socket.recv_multipart()
        server_msg = ServerMessage.parse(response[1])
        msg_data = server_msg.getData()
        self.assertEqual("server_init_failed", msg_data[0])
        self.assertTrue(msg_data[1].startswith("json.decoder.JSONDecodeError:"))
        server.close()

    def test_multiple_client_sockets_sync(self):
        """Tests multiple client sockets pinging the server synchronously (sequentially)."""
        server = EngineServer("tcp", 5558, ServerSecurityModel.NONE, "")
        socket1 = self.client_context.socket(zmq.DEALER)
        socket2 = self.client_context.socket(zmq.DEALER)
        socket3 = self.client_context.socket(zmq.DEALER)
        socket1.connect("tcp://localhost:5558")
        socket2.connect("tcp://localhost:5558")
        socket3.connect("tcp://localhost:5558")
        ping_msg1 = ServerMessage("ping", "1", "", None).to_bytes()
        ping_msg2 = ServerMessage("ping", "2", "", None).to_bytes()
        ping_msg3 = ServerMessage("ping", "3", "", None).to_bytes()
        socket1.send_multipart([ping_msg1])
        response1 = socket1.recv_multipart()
        self.assertEqual(response1[1], ping_msg1)
        socket2.send_multipart([ping_msg2])
        response2 = socket2.recv_multipart()
        self.assertEqual(response2[1], ping_msg2)
        socket3.send_multipart([ping_msg3])
        response3 = socket3.recv_multipart()
        self.assertEqual(response3[1], ping_msg3)
        socket1.close()
        socket2.close()
        socket3.close()
        server.close()

    def test_multiple_client_sockets_async(self):
        """Tests multiple client sockets pinging the server asynchronously."""
        server = EngineServer("tcp", 5559, ServerSecurityModel.NONE, "")
        socket1 = self.client_context.socket(zmq.DEALER)
        socket2 = self.client_context.socket(zmq.DEALER)
        socket3 = self.client_context.socket(zmq.DEALER)
        socket1.connect("tcp://localhost:5559")
        socket2.connect("tcp://localhost:5559")
        socket3.connect("tcp://localhost:5559")
        ping_msg1 = ServerMessage("ping", "1", "", None).to_bytes()
        ping_msg2 = ServerMessage("ping", "2", "", None).to_bytes()
        ping_msg3 = ServerMessage("ping", "3", "", None).to_bytes()
        socket1.send_multipart([ping_msg1])
        socket2.send_multipart([ping_msg2])
        socket3.send_multipart([ping_msg3])
        response1 = socket1.recv_multipart()
        response2 = socket2.recv_multipart()
        response3 = socket3.recv_multipart()
        self.assertEqual(response1[1], ping_msg1)
        self.assertEqual(response2[1], ping_msg2)
        self.assertEqual(response3[1], ping_msg3)
        socket1.close()
        socket2.close()
        socket3.close()
        server.close()

    def test_engineserver_close(self):
        """Tests thread, socket, and context states after server has been closed."""
        server = EngineServer("tcp", 5555, ServerSecurityModel.NONE, "")
        self.assertFalse(server.ctrl_msg_sender.closed)
        self.assertFalse(server._context.closed)
        self.assertTrue(server.is_alive())
        server.close()
        self.assertTrue(server.ctrl_msg_sender.closed)  # PAIR socket should be closed
        self.assertTrue(server._context.closed)  # Context should be closed
        self.assertFalse(server.is_alive())  # server thread should not be alive
        self.assertTrue(all(thread.name != "EngineServerThread" for thread in threading.enumerate()))


if __name__ == "__main__":
    unittest.main()
