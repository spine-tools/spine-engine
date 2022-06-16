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
Contains EngineServer class for running a Zero-MQ server with Spine Engine.
:authors: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   19.08.2021
"""
import json.decoder
import os
import time
import threading
import ipaddress
import enum
import zmq
from zmq.auth.thread import ThreadAuthenticator
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.request import Request
from spine_engine.server.remote_execution_handler import RemoteExecutionHandler
from spine_engine.server.remote_ping_handler import RemotePingHandler


class ServerSecurityModel(enum.Enum):
    NONE = 0
    STONEHOUSE = 1


class EngineServer(threading.Thread):
    """A server for receiving execution requests from Spine Toolbox."""

    def __init__(self, protocol, port, sec_model, sec_folder):
        """Initializes the server.

        Args:
            protocol (str): Protocol to be used by the server.
            port (int): Port to bind the server to
            sec_model (ServerSecurityModel): Security model state
            sec_folder (str): Folder, where security files have been stored.
        """
        try:
            if sec_model == ServerSecurityModel.NONE:
                self._sec_model_state = ServerSecurityModel.NONE
            elif sec_model == ServerSecurityModel.STONEHOUSE:
                if not sec_folder:
                    raise ValueError("EngineServer(): security folder input is missing.")
                base_dir = sec_folder
                self.keys_dir = os.path.join(base_dir, 'certificates')
                self.public_keys_dir = os.path.join(base_dir, 'public_keys')
                self.secret_keys_dir = os.path.join(base_dir, 'private_keys')
                if not os.path.exists(self.keys_dir):
                    raise ValueError(f"Security folder: {self.keys_dir} does not exist")
                elif not os.path.exists(self.public_keys_dir):
                    raise ValueError(f"Security folder: {self.public_keys_dir} does not exist")
                elif not os.path.exists(self.secret_keys_dir):
                    raise ValueError(f"Security folder: {self.secret_keys_dir} does not exist")
                self._sec_folder = sec_folder
                self._sec_model_state = ServerSecurityModel.STONEHOUSE
        except Exception as e:
            raise ValueError(f"Invalid input. Error: {e}")
        self.protocol = protocol
        self.port = port
        self.auth = None
        # NOTE: Contexts are thread-safe! Sockets are NOT!. Do not use or close
        # sockets except in the thread that created them.
        self._context = zmq.Context()
        self.ctrl_msg_sender = self._context.socket(zmq.PAIR)
        self.ctrl_msg_sender.bind("inproc://ctrl_msg")  # inproc:// transport requires a bind() before connect()
        # Start serving
        threading.Thread.__init__(self, target=self.serve, name="EngineServerThread")
        self.start()

    def close(self):
        """Closes the server by sending a KILL message to this thread using a PAIR socket."""
        if self.auth is not None:
            self.auth.stop()
            time.sleep(0.2)  # wait a bit until authenticator has been closed
        self.ctrl_msg_sender.send(b"KILL")
        self.ctrl_msg_sender.close()
        self._context.term()

    def serve(self):
        """Creates the required sockets, which are polled asynchronously. The ROUTER socket handles communicating
        with clients, DEALER sockets are for communicating with the backend processes and PAIR sockets are for
        internal server control messages."""
        try:
            frontend = self._context.socket(zmq.ROUTER)
            backend = self._context.socket(zmq.DEALER)
            backend.bind("inproc://backend")
            # Socket for internal control input (i.e. killing the server)
            ctrl_msg_listener = self._context.socket(zmq.PAIR)
            ctrl_msg_listener.connect("inproc://ctrl_msg")
            worker_thread_killer = self._context.socket(zmq.PAIR)
            worker_thread_killer.bind("inproc://worker_ctrl")

            if self._sec_model_state == ServerSecurityModel.STONEHOUSE:
                try:
                    self.auth = self.enable_stonehouse_security(frontend)
                except ValueError:
                    raise
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
                    msg = frontend.recv_multipart()
                    request = self.handle_frontend_message_received(frontend, msg)
                    if not request:
                        print("Received request malformed. - continuing...")
                        continue
                    print(f"New request from client {request.connection_id()}")
                    # worker_id = uuid.uuid4().hex  # TODO: Use this if problems with connection_id
                    if request.cmd() == "execute":
                        worker = RemoteExecutionHandler(self._context, request)
                    elif request.cmd() == "ping":
                        worker = RemotePingHandler(self._context, request)
                    else:
                        print(f"Unknown command {request.cmd()} requested")
                        request.send_error_reply(frontend, f"Error message from server - Unknown command "
                                                           f"'{request.cmd()}' requested")
                        continue
                    worker.start()
                    workers[request.connection_id()] = worker
                if socks.get(backend) == zmq.POLLIN:
                    # Worker has finished execution. Relay reply from backend back to client using the frontend socket
                    message = backend.recv_multipart()
                    print(f"Sending response to client {message[0]}")
                    frontend.send_multipart(message)
                    # Close and delete finished worker
                    finished_worker = workers.pop(message[0])
                    finished_worker.close()
                if socks.get(ctrl_msg_listener) == zmq.POLLIN:
                    if len(workers) > 0:
                        print(f"WARNING: Some workers still running:{workers.keys()}")
                    break
            except Exception as e:
                print(f"EngineServer.serve() exception: {type(e)} serving failed, exception: {e}")
                break
        # Close sockets
        ctrl_msg_listener.close()
        frontend.close()
        backend.close()
        worker_thread_killer.close()

    def handle_frontend_message_received(self, socket, msg):
        """Check received message integrity.

        msg for ping is eg.
        [b'\x00k\x8bEg', b'', b'{\n   "command": "ping",\n   "id":"4773735",\n   "data":"",\n   "files": {}\n}']
        where,
        msg[0] - Frame 1, is the connection identity (or an address). Unique binary string handle to the connection.
        msg[1] - Frame 2, empty delimiter frame.
        msg[2] - Frame 3, data frame. This is the request from client as a binary
            JSON string containing a server message dictionary

        Returns:
            None if something went wrong or a new Request instance
        """
        b_json_str_server_msg = msg[2]  # binary string
        if len(b_json_str_server_msg) <= 10:  # Message size too small
            print(f"User data frame too small [{len(msg[2])}]. msg:{msg}")
            self.send_init_failed_reply(socket, msg[0], f"User data frame too small "
                                                        f"- Malformed message sent to server.")
            return None
        try:
            json_str_server_msg = b_json_str_server_msg.decode("utf-8")  # json string
        except UnicodeDecodeError as e:
            print(f"Decoding received msg '{msg[2]} ' failed. \nUnicodeDecodeError: {e}")
            self.send_init_failed_reply(socket, msg[0], f"UnicodeDecodeError: {e}. "
                                                        f"- Malformed message sent to server.")
            return None
        # Load JSON string into dictionary
        try:
            server_msg = json.loads(json_str_server_msg)  # dictionary
        except json.decoder.JSONDecodeError as e:
            self.send_init_failed_reply(socket, msg[0], f"json.decoder.JSONDecodeError: {e}. "
                                                        f"- Message parsing error at server.")
            return None
        # server_msg is now a dict with keys: 'command', 'id', 'data', and 'files'
        data_str = server_msg["data"]  # String
        files = server_msg["files"]  # Dictionary. TODO: Should this be a list?
        files_list = []
        if len(files) > 0:
            for f in files:
                files_list.append(files[f])
        return Request(msg, server_msg["command"], server_msg["id"], data_str, files_list)

    @staticmethod
    def send_init_failed_reply(socket, connection_id, error_msg):
        """Sends an error reply to client when initialisation fails for some reason.

        Args:
            socket (ZMQSocket): Socket for sending the reply
            connection_id (bytes): Client Id. Assigned by the frontend ROUTER socket when a request is received.
            error_msg (str): Error message to client
        """
        err_msg_as_json = json.dumps(error_msg)
        reply_msg = ServerMessage("", "", err_msg_as_json, [])
        frame = [connection_id, b"", reply_msg.to_bytes()]
        socket.send_multipart(frame)
        print("\nClient has been notified. Moving on...")

    def enable_stonehouse_security(self, frontend):
        """Enables Stonehouse security by starting an authenticator and configuring
        the frontend socket with authenticator.

        implementation based on https://github.com/zeromq/pyzmq/blob/main/examples/security/stonehouse.py

        Args:
            frontend (zmq.Socket): Frontend socket
        """
        auth = ThreadAuthenticator(self._context)  # Start an authenticator for this context
        auth.start()
        endpoints_file = os.path.join(self._sec_folder, "allowEndpoints.txt")
        if not os.path.exists(endpoints_file):
            raise ValueError(
                f"File allowEndpoints.txt missing. Please create the file into directory: "
                f"{self._sec_folder} and add allowed IP's there"
            )
        endpoints = self._read_end_points(endpoints_file)
        if not endpoints:
            raise ValueError("No endpoints configured. Please add allowed IP's into allowEndPoints.txt")
        # Allow configured endpoints
        allowed = list()
        for ep in endpoints:
            try:
                ep = ep.strip()
                ipaddress.ip_address(ep)
                auth.allow(ep)
                allowed.append(ep)  # Just for printing
            except:
                raise ValueError(f"Invalid IP address in allowEndpoints.txt:'{ep}'")
        allowed_str = "\n".join(allowed)
        print(f"StoneHouse security activated. Allowed endpoints ({len(allowed)}):\n{allowed_str}")
        # Tell the authenticator how to handle CURVE requests
        auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)
        server_secret_file = os.path.join(self.secret_keys_dir, "server.key_secret")
        server_public, server_secret = zmq.auth.load_certificate(server_secret_file)
        frontend.curve_secretkey = server_secret
        frontend.curve_publickey = server_public
        frontend.curve_server = True  # must come before bind
        return auth

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
