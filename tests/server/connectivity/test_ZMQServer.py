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
Unit tests for ZMQServer class.
:author: P. Pääkkönen (VTT)
:date:   19.8.2021
"""
import threading
import unittest
import zmq
import time
import os
import pathlib
from spine_engine.server.connectivity.zmq_server import ZMQServer, ZMQSecurityModelState
from spine_engine.server.connectivity.zmq_server_observer import ZMQServerObserver
from spine_engine.server.connectivity.zmq_connection import ZMQConnection

base_dir = os.path.join(str(pathlib.Path(__file__).parent), "secfolder")


def _security_folder_exists():
    """Security folder and allowEndpoints.txt must exist to test security."""
    endpointfile = os.path.join(base_dir, "allowEndpoints.txt")
    return os.path.exists(base_dir) and os.path.exists(endpointfile)


class TestObserver(ZMQServerObserver):
    """Dummy observer that echoes the given message back to sender."""
    def receiveConnection(self, conn: ZMQConnection) -> None:
        conn.send_reply(conn.get_message_parts()[0])


class TestZMQServer(unittest.TestCase):
    def setUp(self):
        """Sets up client context and socket."""
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.base_dir = base_dir
        # self.socket.connect("tcp://localhost:5559")

    def tearDown(self):
        """Closes client socket and context if open."""
        if not self.socket.closed:
            self.socket.close()
        if not self.context.closed:
            self.context.term()

    def test_missing_secfolder(self):
        """Tests the starting/stopping of the ZMQ server without proper sec folder"""
        ob = TestObserver()
        server = None
        with self.assertRaises(ValueError):
            server = ZMQServer("tcp", 6002, ob, ZMQSecurityModelState.STONEHOUSE, "")
        self.assertIsNone(server)

    def test_invalid_secfolder(self):
        """Tests the starting/stopping of the ZMQ server without proper sec folder."""
        ob = TestObserver()
        server = None
        with self.assertRaises(ValueError):
            server = ZMQServer("tcp", 6003, ob, ZMQSecurityModelState.STONEHOUSE, "/fwhkjfnsefkjnselk")
        self.assertIsNone(server)

    @unittest.skipIf(not _security_folder_exists(), "Test requires a security folder")
    def test_starting_server_connection_with_security_tcp(self):
        """Tests starting a ZMQ server with tcp, and reception of a connection and message parts (with security)."""
        ob = TestObserver()
        zmq_server = ZMQServer("tcp", 6006, ob, ZMQSecurityModelState.STONEHOUSE, base_dir)
        # Configure client security
        secret_keys_dir = os.path.join(base_dir, "private_keys")
        keys_dir = os.path.join(base_dir, "certificates")
        public_keys_dir = os.path.join(base_dir, "public_keys")
        # We need two certificates, one for the client and one for the server.
        client_secret_file = os.path.join(secret_keys_dir, "client.key_secret")
        client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
        self.socket.curve_secretkey = client_secret
        self.socket.curve_publickey = client_public
        server_public_file = os.path.join(public_keys_dir, "server.key")
        server_public, _ = zmq.auth.load_certificate(server_public_file)
        # The client must know the server's public key to make a CURVE connection.
        self.socket.curve_serverkey = server_public
        self.socket.connect("tcp://localhost:6006")
        msg_parts = []
        part1 = "feiofnoknfsdnoiknsmd"
        fileArray = bytearray([1, 2, 3, 4, 5])
        part1Bytes = bytes(part1, "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(fileArray)
        self.socket.send_multipart(msg_parts)  # We are sending a msg and expecting the same msg to come back
        msg = self.socket.recv()
        msg_decoded = msg.decode("utf-8")
        self.assertEqual(msg, part1Bytes)  # Check that transmitted and received bytes objects are equal
        self.assertEqual(part1, msg_decoded)  # Check that transmitted and received strings are equal
        zmq_server.close()

    def test_starting_server_no_observer_tcp(self):
        """Tests starting the server without an observer."""
        server = None
        with self.assertRaises(ValueError):
            server = ZMQServer("tcp", 5550, None, ZMQSecurityModelState.NONE, "")
        self.assertIsNone(server)

    def test_starting_server_connection_established_tcp(self):
        """Tests starting of a ZMQ server with tcp, and reception of a connection and message parts."""
        ob = TestObserver()
        server = ZMQServer("tcp", 5556, ob, ZMQSecurityModelState.NONE, "")
        self.socket.connect("tcp://localhost:5556")
        msg_parts = []
        part1 = "feiofnoknfsdnoiknsmd"
        fileArray = bytearray([1, 2, 3, 4, 5])
        part1Bytes = bytes(part1, "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(fileArray)
        self.socket.send_multipart(msg_parts)
        msg = self.socket.recv()
        msg_decoded = msg.decode("utf-8")
        self.assertEqual(msg, part1Bytes)  # Check that transmitted and received bytes objects are equal
        self.assertEqual(part1, msg_decoded)  # Check that transmitted and received strings are equal
        server.close()

    def test_multiple_data_items_tcp(self):
        """Tests transfer of multiple data items within a connection."""
        ob = TestObserver()
        server = ZMQServer("tcp", 6000, ob, ZMQSecurityModelState.NONE, "")
        # Connect to the server
        self.socket.connect("tcp://localhost:6000")
        msg_parts = []
        part1 = "feiofnoknfsdnoiknsmd"
        fileArray = bytearray([1, 2, 3, 4, 5])
        part1Bytes = bytes(part1, "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(fileArray)
        j = 0
        while j < 100:
            self.socket.send_multipart(msg_parts)
            msg = self.socket.recv()
            self.assertEqual(msg, part1Bytes)  # check that echoed content is as expected
            j += 1
        server.close()

    def test_multiple_sockets_tcp(self):
        """Tests multiple sockets over TCP."""
        ob = TestObserver()
        server = ZMQServer("tcp", 5558, ob, ZMQSecurityModelState.NONE, "")
        socket1 = self.context.socket(zmq.REQ)
        socket2 = self.context.socket(zmq.REQ)
        socket3 = self.context.socket(zmq.REQ)
        socket1.connect("tcp://localhost:5558")
        socket2.connect("tcp://localhost:5558")
        socket3.connect("tcp://localhost:5558")
        msg_parts = []
        part1 = "feiofnoknfsdnoiknsmd"
        fileArray = bytearray([1, 2, 3, 4, 5])
        part1Bytes = bytes(part1, "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(fileArray)
        # Send over multiple sockets
        socket1.send_multipart(msg_parts)
        msg = socket1.recv()
        self.assertEqual(msg, part1Bytes)  # check that echoed content is as expected
        socket2.send_multipart(msg_parts)
        msg = socket2.recv()
        self.assertEqual(msg, part1Bytes)  # check that echoed content is as expected
        socket3.send_multipart(msg_parts)
        msg = socket3.recv()
        self.assertEqual(msg, part1Bytes)  # check that echoed content is as expected
        socket1.close()
        socket2.close()
        socket3.close()
        server.close()

    def test_multiple_sockets_seq_transfer_tcp(self):
        """Tests multiple sockets over TCP."""

        ob = TestObserver()
        server = ZMQServer("tcp", 5559, ob, ZMQSecurityModelState.NONE, "")
        socket1 = self.context.socket(zmq.REQ)
        socket2 = self.context.socket(zmq.REQ)
        socket3 = self.context.socket(zmq.REQ)
        socket1.connect("tcp://localhost:5559")
        socket2.connect("tcp://localhost:5559")
        socket3.connect("tcp://localhost:5559")
        msg_parts = []
        part1 = "feiofnoknfsdnoiknsmd"
        fileArray = bytearray([1, 2, 3, 4, 5])
        part1Bytes = bytes(part1, "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(fileArray)
        # send over multiple sockets
        socket1.send_multipart(msg_parts)
        # print("test_multiple_sockets_seq_transfer_tcp() msg on socket 1 sent.")
        socket2.send_multipart(msg_parts)
        # print("test_multiple_sockets_seq_transfer_tcp() msg on socket 2 sent.")
        socket3.send_multipart(msg_parts)
        # print("test_multiple_sockets_seq_transfer_tcp() msg on socket 3 sent.")
        msg = socket1.recv()
        # print("test_multiple_sockets_seq_transfer_tcp() msg received: %s"%msg)
        self.assertEqual(msg, part1Bytes)  # check that echoed content is as expected
        msg = socket2.recv()
        # print("test_multiple_sockets_seq_transfer_tcp() msg received: %s"%msg)
        self.assertEqual(msg, part1Bytes)  # check that echoed content is as expected
        msg = socket3.recv()
        # print("test_multiple_sockets_seq_transfer_tcp() msg received: %s"%msg)
        self.assertEqual(msg, part1Bytes)  # check that echoed content is as expected
        socket1.close()
        socket2.close()
        socket3.close()
        server.close()

    def test_zmqserver_threading(self):
        """Tests thread, socket, and context states after server has been closed."""
        ob = TestObserver()
        server = ZMQServer("tcp", 5555, ob, ZMQSecurityModelState.NONE, "")
        time.sleep(1)
        self.assertFalse(server.ctrl_msg_sender.closed)
        self.assertFalse(server._context.closed)
        self.assertTrue(server.is_alive())
        server.close()
        self.assertTrue(server.ctrl_msg_sender.closed)  # PAIR socket should be closed
        self.assertTrue(server._context.closed)  # Context should be closed
        self.assertFalse(server.is_alive())  # server thread should not be alive

    def test_sequential_transmit_using_req_socket_fails(self):
        """Tests that two sends in a row for a REQ socket fails. Allowed send/receive
        pattern for REQ socket is send, receive, send, receive, etc."""
        ob = TestObserver()
        server = ZMQServer("tcp", 5557, ob, ZMQSecurityModelState.NONE, "")
        socket = self.context.socket(zmq.REQ)
        socket.connect("tcp://localhost:5557")
        socket.setsockopt(zmq.LINGER, 1)
        msg_parts = []
        part1 = "feiofnoknfsdnoiknsmd"
        fileArray = bytearray([1, 2, 3, 4, 5])
        part1Bytes = bytes(part1, "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(fileArray)
        socket.send_multipart(msg_parts)
        with self.assertRaises(zmq.ZMQError):
            socket.send_multipart(msg_parts)
        socket.close()
        server.close()


if __name__ == "__main__":
    unittest.main()
