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
Contains EngineServer class for running a Zero-MQ server with Spine Engine.
"""
import enum
import ipaddress
import json.decoder
import os
import queue
import threading
import uuid
import zmq
from zmq.auth.thread import ThreadAuthenticator
from spine_engine.server.persistent_execution_service import PersistentExecutionService
from spine_engine.server.ping_service import PingService
from spine_engine.server.project_extractor_service import ProjectExtractorService
from spine_engine.server.project_remover_service import ProjectRemoverService
from spine_engine.server.project_retriever_service import ProjectRetrieverService
from spine_engine.server.remote_execution_service import RemoteExecutionService
from spine_engine.server.request import Request
from spine_engine.server.util.server_message import ServerMessage


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
        super().__init__(target=self.serve, name="EngineServerThread")
        if sec_model == ServerSecurityModel.NONE:
            self._sec_model_state = ServerSecurityModel.NONE
        elif sec_model == ServerSecurityModel.STONEHOUSE:
            if not sec_folder:
                raise ValueError("Path to security folder missing")
            base_dir = sec_folder
            self.keys_dir = os.path.join(base_dir, "certificates")
            self.public_keys_dir = os.path.join(base_dir, "public_keys")
            self.secret_keys_dir = os.path.join(base_dir, "private_keys")
            if not os.path.exists(self.keys_dir):
                raise ValueError(f"Security folder: {self.keys_dir} does not exist")
            if not os.path.exists(self.public_keys_dir):
                raise ValueError(f"Security folder: {self.public_keys_dir} does not exist")
            if not os.path.exists(self.secret_keys_dir):
                raise ValueError(f"Security folder: {self.secret_keys_dir} does not exist")
            self._sec_folder = sec_folder
            self._sec_model_state = ServerSecurityModel.STONEHOUSE
        self.protocol = protocol
        self.port = port
        self.auth = None
        # NOTE: Contexts are thread-safe! Sockets are NOT! Do not use or close
        # sockets except in the thread that created them.
        self._context = zmq.Context()
        self.ctrl_msg_sender = self._context.socket(zmq.PAIR)
        self.ctrl_msg_sender.bind("inproc://ctrl_msg")  # inproc:// transport requires a bind() before connect()
        self.persistent_exec_mngrs = dict()
        self.start()  # Start serving

    def close(self):
        """Closes the server by sending a KILL message to this thread using a PAIR socket."""
        if self.auth is not None:
            self.auth.stop()
            while self.auth.is_alive():
                pass
        self.ctrl_msg_sender.send(b"KILL")
        self.join()  # Wait for the thread to finish and sockets to close
        self.ctrl_msg_sender.close()  # Close this in the same thread that it was created in
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
        project_dirs = dict()  # Mapping of job Id to an abs. path to a project directory ready for execution
        persistent_exec_mngr_q = queue.Queue()
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
                    print(f"{request.cmd().upper()} request from client {request.connection_id()}")
                    job_id = uuid.uuid4().hex  # Job Id for execution worker
                    if request.cmd() == "ping":
                        worker = PingService(self._context, request, job_id)
                    elif request.cmd() == "prepare_execution":
                        worker = ProjectExtractorService(self._context, request, job_id)
                    elif request.cmd() == "start_execution":
                        project_dir = project_dirs.get(request.request_id(), None)  # Get project dir based on job_id
                        if not project_dir:
                            print(f"Project for job_id:{request.request_id()} not found")
                            msg = (
                                f"Starting DAG execution failed. Project directory for "
                                f"job_id:{request.request_id()} not found."
                            )
                            self.send_init_failed_reply(frontend, request.connection_id(), msg)
                            continue
                        worker = RemoteExecutionService(
                            self._context,
                            request,
                            job_id,
                            project_dir,
                            persistent_exec_mngr_q,
                            self.port,
                        )
                    elif request.cmd() == "stop_execution":
                        worker = workers.get(request.request_id(), None)  # Get DAG execution worker based on job Id
                        if not worker:
                            print(f"Worker for job_id:{request.request_id()} not found")
                            msg = f"Stopping DAG execution failed. Worker for job_id:{request.request_id()} not found."
                            self.send_init_failed_reply(frontend, request.connection_id(), msg)
                            continue
                        worker.stop_engine()
                        request.send_response(frontend, ("server_status_msg", "DAG worker stopped"))
                        continue
                    elif request.cmd() == "answer_prompt":
                        worker = workers.get(request.request_id(), None)  # Get DAG execution worker based on job Id
                        if not worker:
                            print(f"Worker for job_id:{request.request_id()} not found")
                            msg = f"Answering prompt failed. Worker for job_id:{request.request_id()} not found."
                            self.send_init_failed_reply(frontend, request.connection_id(), msg)
                            continue
                        prompter_id, answer = request.data()
                        worker.answer_prompt(prompter_id, answer)
                        continue
                    elif request.cmd() == "retrieve_project":
                        project_dir = project_dirs.get(request.request_id(), None)  # Get project dir based on job_id
                        if not project_dir:
                            print(f"Project for job_id:{request.request_id()} not found")
                            msg = (
                                f"Retrieving project for job_id {request.request_id()} failed. "
                                f"Project directory not found."
                            )
                            self.send_init_failed_reply(frontend, request.connection_id(), msg)
                            continue
                        worker = ProjectRetrieverService(self._context, request, job_id, project_dir)
                    elif request.cmd() == "execute_in_persistent":
                        exec_mngr_key = request.data()[0]
                        exec_mngr = self.persistent_exec_mngrs.get(exec_mngr_key, None)
                        if not exec_mngr:
                            print(f"Persistent exec. mngr for key:{exec_mngr_key} not found.")
                            msg = (
                                f"Executing command:{request.data()[1]} - {request.data()[2]} in persistent "
                                f"manager failed. Persistent execution manager for key {exec_mngr_key} not found."
                            )
                            self.send_init_failed_reply(frontend, request.connection_id(), msg)
                            continue
                        worker = PersistentExecutionService(self._context, request, job_id, exec_mngr, self.port)
                    elif request.cmd() == "remove_project":
                        project_dir = project_dirs.get(request.request_id(), None)  # Get project dir based on job_id
                        if not project_dir:
                            print(f"Project for job_id:{request.request_id()} not found")
                            msg = f"Project directory for job_id {request.request_id()} not found"
                            self.send_init_failed_reply(frontend, request.connection_id(), msg)
                            continue
                        worker = ProjectRemoverService(self._context, request, job_id, project_dir)
                    else:
                        print(f"Unknown command {request.cmd()} requested")
                        msg = f"Server error: Unknown command '{request.cmd()}' requested"
                        self.send_init_failed_reply(frontend, request.connection_id(), msg)
                        continue
                    worker.start()
                    workers[job_id] = worker
                if socks.get(backend) == zmq.POLLIN:
                    # Worker has finished execution. Relay reply from backend back to client using the frontend socket
                    message = backend.recv_multipart()
                    internal_msg = json.loads(message.pop().decode("utf-8"))
                    if internal_msg[1] != "in_progress":
                        finished_worker = workers.pop(internal_msg[0])
                        if isinstance(finished_worker, ProjectExtractorService):
                            project_dirs[internal_msg[0]] = internal_msg[1]
                        if isinstance(finished_worker, RemoteExecutionService):
                            # Store refs to exec. managers
                            try:
                                new_exec_mngrs = persistent_exec_mngr_q.get_nowait()
                                for k, v in new_exec_mngrs.items():
                                    self.persistent_exec_mngrs[k] = v
                            except queue.Empty:
                                pass
                        finished_worker.close()
                        finished_worker.join()
                    print(f"Sending msg to client {message[0]}")
                    frontend.send_multipart(message)
                if socks.get(ctrl_msg_listener) == zmq.POLLIN:
                    print("Closing server...")
                    self.kill_persistent_exec_mngrs()
                    if len(workers) > 0:
                        print(f"WARNING: Some workers still running:{workers.keys()}")
                    break
            except Exception as e:
                print(f"[DEBUG] EngineServer.serve() exception: {type(e)} serving failed, exception: {e}")
                break
        # Close sockets
        ctrl_msg_listener.close()
        frontend.close()
        backend.close()

    def kill_persistent_exec_mngrs(self):
        """Kills all persistent (execution) manager processes."""
        n_exec_mngrs = len(self.persistent_exec_mngrs)
        if n_exec_mngrs > 0:
            print(f"Closing {len(self.persistent_exec_mngrs)} persistent execution manager processes")
            for k, exec_mngr in self.persistent_exec_mngrs.items():
                exec_mngr._persistent_manager.kill_process()
            self.persistent_exec_mngrs.clear()

    def handle_frontend_message_received(self, socket, msg):
        """Check received message integrity.

        msg for ping is eg.
        [b'\x00k\x8bEg', b'{\n   "command": "ping",\n   "id":"4773735",\n   "data":"",\n   "files": {}\n}']
        where,
        msg[0] - Frame 1, is the connection identity (or an address). Unique binary string handle to the connection.
        msg[1] - Frame 2, data frame. This is the request from client as a binary
            JSON string containing a server message dictionary

        Returns:
            None if something went wrong or a new Request instance
        """
        b_json_str_server_msg = msg[1]  # binary string
        if len(b_json_str_server_msg) <= 10:  # Message size too small
            print(f"User data frame too small [{len(msg[1])}]. msg:{msg}")
            self.send_init_failed_reply(socket, msg[0], "User data frame too small. Malformed message sent to server.")
            return None
        try:
            json_str_server_msg = b_json_str_server_msg.decode("utf-8")  # json string
        except UnicodeDecodeError as e:
            print(f"Decoding received msg '{msg[1]} ' failed. \nUnicodeDecodeError: {e}")
            self.send_init_failed_reply(
                socket, msg[0], f"UnicodeDecodeError: {e}. " f"- Malformed message sent to server."
            )
            return None
        # Load JSON string into dictionary
        try:
            server_msg = json.loads(json_str_server_msg)  # dictionary
        except json.decoder.JSONDecodeError as e:
            self.send_init_failed_reply(
                socket, msg[0], f"json.decoder.JSONDecodeError: {e}. " f"- Message parsing error at server."
            )
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
        """Sends an error reply to client when request is malformed.

        Args:
            socket (ZMQSocket): Socket for sending the reply
            connection_id (bytes): Client Id. Assigned by the frontend ROUTER socket when a request is received.
            error_msg (str): Error message to client
        """
        error_msg_tuple = ("server_init_failed", error_msg)
        err_msg_as_json = json.dumps(error_msg_tuple)
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
        auth.configure_curve(domain="*", location=zmq.auth.CURVE_ALLOW_ANY)
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
