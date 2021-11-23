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

import zmq
import threading
import time
import os
import ipaddress
from enum import unique, Enum
from zmq.auth.thread import ThreadAuthenticator
from .zmq_connection import ZMQConnection


@unique
class ZMQSecurityModelState(Enum):
    NONE = 0  # ZMQ can be executed without security
    STONEHOUSE = 1  # stonehouse-security model of Zero-MQ


class ZMQServer(threading.Thread):
    """A server implementation for receiving connections (ZMQConnection) from the Spine Toolbox."""

    def __init__(self, protocol, port, zmqServerObserver, secModel, secFolder):
        """
        Initialises the server.
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
        self.ctrl_msg_sender.close()
        self.join()
        self._context.term()

    def serve(self):
        """Creates two sockets, which are both polled asynchronously. The REPLY socket handles messages
        from Spine Toolbox client, while the PAIR socket listens for 'server' internal control messages."""
        try:
            client_msg_listener = self._context.socket(zmq.REP)
            # Socket for internal control input (i.e. killing the server)
            ctrl_msg_listener = self._context.socket(zmq.PAIR)
            ctrl_msg_listener.connect("inproc://ctrl_msg")
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
                client_msg_listener.curve_secretkey = server_secret
                client_msg_listener.curve_publickey = server_public
                client_msg_listener.curve_server = True  # must come before bind
            client_msg_listener.bind(self.protocol + "://*:" + str(self.port))
            poller = zmq.Poller()
            poller.register(client_msg_listener, zmq.POLLIN)
            poller.register(ctrl_msg_listener, zmq.POLLIN)
        except Exception as e:
            # print("ZMQServer couldn't be started due to exception: %s"%e)
            raise ValueError("Invalid input ZMQServer._receive()")
        while True:
            try:
                socks = dict(poller.poll())
                if socks.get(client_msg_listener) == zmq.POLLIN:
                    msg_parts = client_msg_listener.recv_multipart()
                    self._conn = ZMQConnection(client_msg_listener, msg_parts)
                    self._observer.receiveConnection(self._conn)
                if socks.get(ctrl_msg_listener) == zmq.POLLIN:
                    # No need to check the message content.
                    break
            except Exception as e:
                print(f"ZMQServer.serve() exception: {type(e)} serving failed, exception: {e}")
                break
        # Close sockets and connections
        if self._secModelState == ZMQSecurityModelState.STONEHOUSE:
            auth.stop()
            # wait a bit until authenticator has been closed
            time.sleep(0.2)  # TODO: Check if this is necessary
        ctrl_msg_listener.close()
        client_msg_listener.close()


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
