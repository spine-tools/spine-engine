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
Contains ZMQServer class for running a Zero-MQ server with Spine Engine.
:authors: P. Pääkkönen (VTT)
:date:   19.08.2021
"""
import json.decoder
import os
import time
import threading
import ipaddress
import uuid
from enum import unique, Enum
import zmq
from zmq.auth.thread import ThreadAuthenticator
from spine_engine.server.connectivity.zmq_connection import ZMQConnection
from spine_engine.server.remote_connection_handler import RemoteConnectionHandler
from spine_engine.server.remote_ping_handler import RemotePingHandler


@unique
class ZMQSecurityModelState(Enum):
    NONE = 0  # ZMQ can be executed without security
    STONEHOUSE = 1  # stonehouse-security model of Zero-MQ


class ZMQServer(threading.Thread):
    """A server implementation for receiving connections (ZMQConnection) from the Spine Toolbox."""

    def __init__(self, protocol, port, zmqServerObserver, secModel, secFolder):
        """Initialises the server.

        Args:
            protocol: protocol to be used by the server.
            port: port to bind the server to
            secModel: see: ZMQSecurityModelState 
            secFolder: folder, where security files have been stored.
        """
        if not zmqServerObserver:
            raise ValueError("Invalid input ZMQServer: No zmqServerObserver given")
        self._observer = zmqServerObserver
        try:
            if secModel == ZMQSecurityModelState.NONE:
                self._secModelState = ZMQSecurityModelState.NONE
            elif secModel == ZMQSecurityModelState.STONEHOUSE:
                # implementation based on https://github.com/zeromq/pyzmq/blob/main/examples/security/stonehouse.py
                if not secFolder:
                    raise ValueError("ZMQServer(): security folder input is missing.")
                # print("beginning to configure security for stonehouse-model of ZMQ")
                base_dir = secFolder
                self.keys_dir = os.path.join(base_dir, 'certificates')
                self.public_keys_dir = os.path.join(base_dir, 'public_keys')
                self.secret_keys_dir = os.path.join(base_dir, 'private_keys')
                if not os.path.exists(self.keys_dir):
                    raise ValueError(f"Security folder: {self.keys_dir} does not exist")
                elif not os.path.exists(self.public_keys_dir):
                    raise ValueError(f"Security folder: {self.public_keys_dir} does not exist")
                elif not os.path.exists(self.secret_keys_dir):
                    raise ValueError(f"Security folder: {self.secret_keys_dir} does not exist")
                self.secFolder = secFolder
                self._secModelState = ZMQSecurityModelState.STONEHOUSE
        except Exception as e:
            raise ValueError(f"Invalid input. Error: {e}")
        self.protocol = protocol
        self.port = port
        # NOTE: Contexts are thread-safe! Sockets are NOT!. Do not use or close
        # sockets except in the thread that created them.
        self._context = zmq.Context()
        self.ctrl_msg_sender = self._context.socket(zmq.PAIR)
        self.ctrl_msg_sender.bind("inproc://ctrl_msg")  # inproc:// transport requires a bind() before connect()
        # Start serving
        threading.Thread.__init__(self, target=self.serve)
        self.name = "Server Thread"
        self.start()

    def close(self):
        """Closes the server by sending a KILL message to receiver thread using a 0MQ socket."""
        self.ctrl_msg_sender.send(b"KILL")
        print("Closing ctrl_msg_sender and context")
        self.ctrl_msg_sender.close()

    def serve(self):
        """Creates two sockets, which are both polled asynchronously. The REPLY socket handles messages
        from Spine Toolbox client, while the PAIR socket listens for 'server' internal control messages."""
        try:
            print(f"serve(): {threading.current_thread()}")
            frontend = self._context.socket(zmq.ROUTER)  # frontend
            backend = self._context.socket(zmq.DEALER)
            backend.bind("inproc://backend")
            # Socket for internal control input (i.e. killing the server)
            ctrl_msg_listener = self._context.socket(zmq.PAIR)
            ctrl_msg_listener.connect("inproc://ctrl_msg")
            worker_thread_killer = self._context.socket(zmq.PAIR)
            worker_thread_killer.bind("inproc://worker_ctrl")
            auth = None
            if self._secModelState == ZMQSecurityModelState.STONEHOUSE:
                # Start an authenticator for this context
                auth = ThreadAuthenticator(self._context)
                auth.start()
                endpoints_file = os.path.join(self.secFolder, "allowEndpoints.txt")
                if not os.path.exists(endpoints_file):
                    raise ValueError(
                        f"File allowEndpoints.txt missing. Please create the file into directory: "
                        f"{self.secFolder} and add allowed IP's there"
                    )
                endpoints = self._read_end_points(endpoints_file)  # read allowed endpoints from file
                if not endpoints:
                    raise ValueError("No end points configured. Please add allowed IP's into allowEndPoints.txt")
                # allow configured endpoints
                allowed = list()
                for ep in endpoints:
                    try:
                        ep = ep.strip()
                        ipaddress.ip_address(ep)
                        auth.allow(ep)
                        allowed.append(ep)  # Just for printing
                    except:
                        print("Invalid IP address in allowEndpoints.txt:'{ep}'")
                allowed_str = "\n".join(allowed)
                print(f"StoneHouse security activated. Allowed end points ({len(allowed)}):\n{allowed_str}")
                # Tell the authenticator how to handle CURVE requests
                auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)
                server_secret_file = os.path.join(self.secret_keys_dir, "server.key_secret")
                server_public, server_secret = zmq.auth.load_certificate(server_secret_file)
                frontend.curve_secretkey = server_secret
                frontend.curve_publickey = server_public
                frontend.curve_server = True  # must come before bind
            frontend.bind(self.protocol + "://*:" + str(self.port))
            poller = zmq.Poller()
            poller.register(frontend, zmq.POLLIN)
            poller.register(backend, zmq.POLLIN)
            poller.register(ctrl_msg_listener, zmq.POLLIN)
        except Exception as e:
            raise ValueError(f"Initializing serve() failed due to exception: {e}")
        workers = dict()
        while True:
            try:
                socks = dict(poller.poll())
                # Broker
                if socks.get(frontend) == zmq.POLLIN:
                    # Frontend received a message, send it to backend for processing
                    print("Frontend received a message")
                    msg = frontend.recv_multipart()
                    connection = self.handle_frontend_message_received(frontend, msg)
                    if not connection:
                        print("Received request malformed. - continuing...")
                        continue
                    # TODO: Make a worker according to the command.
                    worker_id = uuid.uuid4().hex  # TODO: Use connection._connection_id instead
                    if connection.cmd() == "execute":
                        worker = RemoteConnectionHandler(self._context, connection)
                        worker.do_execution()
                    elif connection.cmd() == "ping":
                        worker = RemotePingHandler.handle_ping(connection)
                    else:
                        print(f"Unknown command {connection.cmd()} requested")
                        connection.send_error_reply(f"Error message from server - Unknown command "
                                                    f"'{connection.cmd()}' requested")
                        continue
                    # workers[worker_id] = worker
                    # backend.send_multipart(msg)
                if socks.get(backend) == zmq.POLLIN:
                    # Get reply message from backend and send it back to client
                    message = backend.recv_multipart()
                    frontend.send_multipart(message)
                    # TODO: Remove worker thread from list
                if socks.get(ctrl_msg_listener) == zmq.POLLIN:
                    # print("Closing down worker thread")
                    print(f"workers running:{len(workers)}")
                    # worker_thread_killer.send(b"KILL")
                    # No need to check the message content.
                    break
            except Exception as e:
                print(f"ZMQServer.serve() exception: {type(e)} serving failed, exception: {e}")
                break
        # Close sockets and connections
        if self._secModelState == ZMQSecurityModelState.STONEHOUSE:
            auth.stop()
            time.sleep(0.2)  # wait a bit until authenticator has been closed
        ctrl_msg_listener.close()
        frontend.close()
        backend.close()
        worker_thread_killer.close()
        self._context.term()

    @staticmethod
    def handle_frontend_message_received(socket, msg):
        """Check received message integrity.

        msg for ping is eg.
        [b'\x00k\x8bEg', b'', b'{\n   "command": "ping",\n   "id":"4773735",\n   "data":"",\n   "files": {}\n}']
        where,
        msg[0] - Frame 1, is the connection identity (or an address). Unique binary string handle to the connection.
        msg[1] - Frame 2, empty delimiter frame.
        msg[2] - Frame 3, data frame. This is the request from client as a binary
            JSON string containing a server message dictionary

        Returns:
            None if something went wrong or a new ZMQConnection instance
        """
        print(f"len(msg):{len(msg)}")
        b_json_str_server_msg = msg[2]  # binary string
        if len(b_json_str_server_msg) <= 10:  # Message size too small
            print(f"User data frame too small [{len(msg[2])}]. msg:{msg}")
            ZMQConnection.send_init_failed_reply(socket, f"User data frame too small "
                                                         f"- Malformed message sent to server.")
            return None
        try:
            json_str_server_msg = b_json_str_server_msg.decode("utf-8")  # json string
        except UnicodeDecodeError as e:
            print(f"Decoding received msg '{msg[2]} ' failed. \nUnicodeDecodeError: {e}")
            ZMQConnection.send_init_failed_reply(socket, f"UnicodeDecodeError: {e}. "
                                                         f"- Malformed message sent to server.")
            return None
        # Load JSON string into dictionary
        try:
            server_msg = json.loads(json_str_server_msg)  # dictionary
        except json.decoder.JSONDecodeError as e:
            ZMQConnection.send_init_failed_reply(socket, f":json.decoder.JSONDecodeError: {e}. "
                                                         f"- Message parsing error at server.")
            return None
        # server_msg is now a dict with keys: 'command', 'id', 'data', and 'files'
        print(f"server_msg keys:{server_msg.keys()}")
        data_str = server_msg["data"]  # String
        files = server_msg["files"]  # Dictionary. TODO: Should this be a list?
        files_list = []
        if len(files) > 0:
            for f in files:
                files_list.append(files[f])
        connection = ZMQConnection(msg, socket, server_msg["command"], server_msg["id"], data_str, files_list)
        return connection

    @staticmethod
    def _read_end_points(config_file_location):
        """Reads all lines from a text file and returns them in a list. Empty strings are removed from the list.

        Args:
            config_file_location (str): Full path to some text file

        Returns:
            list: Lines of the given file in a list
        """
        with open(config_file_location, "r") as f:
            all_lines = f.read().splitlines()
            lines = [x for x in all_lines if x]
            return lines


class BackendWorkerManager:
    def __init__(self):
        self._worker_threads = dict()
