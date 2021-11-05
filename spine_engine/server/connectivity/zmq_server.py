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
from enum import unique, Enum
import threading
from .zmq_connection import ZMQConnection
import time
import os
import ipaddress
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator


@unique
class ZMQServerState(Enum):
    RUNNING = 1
    STOPPED = 2


@unique
class ZMQSecurityModelState(Enum):
    NONE = 0  # ZMQ can be executed without security
    STONEHOUSE = 1  # stonehouse-security model of Zero-MQ


class ZMQServer(threading.Thread):
    """A server implementation for receiving connections(ZMQConnection) from the Spine Toolbox."""

    def __init__(self, protocol, port, zmqServerObserver, secModel, secFolder):
        """
        Initialises the server.
        Args:
            protocol: protocol to be used by the server.
            port: port to bind the server to
            secModel: see: ZMQSecurityModelState 
            secFolder: folder, where security files have been stored.
        """

        if zmqServerObserver == None:
            raise ValueError("Invalid input ZMQServer: zmqServerObserver")
        self._observer = zmqServerObserver
        # threading.Thread.__init__(self)
        # self.start()
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
                if not (
                    os.path.exists(self.keys_dir)
                    and os.path.exists(self.public_keys_dir)
                    and os.path.exists(self.secret_keys_dir)
                ):
                    raise ValueError("invalid certificate folders at ZMQServer()")
                self.secFolder = secFolder
                self._secModelState = ZMQSecurityModelState.STONEHOUSE
        except Exception as e:
            # print("ZMQServer couldn't be started due to exception: %s"%e)
            self._state = ZMQServerState.STOPPED
            raise ValueError("Invalid input ZMQServer.")
        self.protocol = protocol
        self.port = port
        # print("ZMQServer started with protocol %s to port %d"%(protocol,port))
        threading.Thread.__init__(self)
        self.start()

    def close(self):
        """Closes the server.

        Returns:
            int: 0 on success, -1 otherwise
        """
        if self._state == ZMQServerState.RUNNING:
            if self._secModelState == ZMQSecurityModelState.STONEHOUSE:
                self._auth.stop()
                # print("ZMQServer.close(): stopped security authenticator.")
                time.sleep(0.2)  # wait a bit until authenticator has been closed
            ret = self._socket.close()
            # print("ZMQServer.close(): socket closed.")
            # time.sleep(1)
            self._zmqContext.term()
            # self._zmqContext.destroy()
            # print("ZMQServer.close(): ZMQ context closed at port %d"%self.port)
            self._state = ZMQServerState.STOPPED
            return 0
        else:
            print("ZMQServer is not running (port %d), cannot close." % self.port)
            return -1

    def run(self):
        self._state = ZMQServerState.RUNNING
        self._receive_data()

    def _receive_data(self):
        """Receives data from the socket, and creates new connections."""
        # initialise Zero-MQ context and bind to a port depending on the security model
        try:
            if self._secModelState == ZMQSecurityModelState.NONE:
                self._zmqContext = zmq.Context()
                self._socket = self._zmqContext.socket(zmq.REP)
                ret = self._socket.bind(self.protocol + "://*:" + str(self.port))
            elif self._secModelState == ZMQSecurityModelState.STONEHOUSE:
                self._zmqContext = zmq.Context.instance()
                # Start an authenticator for this context.
                self._auth = ThreadAuthenticator(self._zmqContext)
                self._auth.start()
                endpoints_file = os.path.join(self.secFolder, "allowEndpoints.txt")
                if not os.path.exists(endpoints_file):
                    raise ValueError(
                        f"File allowEndpoints.txt missing. Please create the file into directory: "
                        f"{self.secFolder} and add allowed IP's there"
                    )
                endpoints = self._readEndpoints(endpoints_file)  # read endpoints to allow
                endpoints = [x for x in endpoints if x]  # Remove empty string(s) from endpoint list
                if not endpoints:
                    self._state = ZMQServerState.STOPPED
                    raise ValueError("No end points configured. Please add allowed IP's into allowEndPoints.txt")
                # allow configured endpoints
                allowed = list()
                for ep in endpoints:
                    try:
                        ep = ep.strip()
                        ipaddress.ip_address(ep)
                        self._auth.allow(ep)
                        allowed.append(ep)  # Just for printing
                    except:
                        print("Invalid IP address in allowEndpoints.txt:'{ep}'")
                allowed_str = "\n".join(allowed)
                print(f"StoneHouse security activated. Allowed end points ({len(allowed)}):\n{allowed_str}")
                # print("ZMQServer(): started authenticator.")
                # Tell the authenticator how to handle CURVE requests
                self._auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)
                self._socket = self._zmqContext.socket(zmq.REP)
                # self._socket.setsockopt(zmq.LINGER, 0)
                server_secret_file = os.path.join(self.secret_keys_dir, "server.key_secret")
                server_public, server_secret = zmq.auth.load_certificate(server_secret_file)
                self._socket.curve_secretkey = server_secret
                self._socket.curve_publickey = server_public
                self._socket.curve_server = True  # must come before bind
                # print("ZMQServer(): binding server and listening (with stonehouse security configured)..")
                self._socket.bind(self.protocol + "://*:" + str(self.port))
        except Exception as e:
            # print("ZMQServer couldn't be started due to exception: %s"%e)
            self._state = ZMQServerState.STOPPED
            raise ValueError("Invalid input ZMQServer._receive()")
        # start listening..
        print("\nListening...")
        while self._state == ZMQServerState.RUNNING:
            try:
                # print("ZMQServer._receive_data(): Starting listening..")
                msg_parts = self._socket.recv_multipart()
                self._conn = ZMQConnection(self._socket, msg_parts)
                self._observer.receiveConnection(self._conn)
                # print("ZMQServer._receive_data(): Received multi-part data.")
                time.sleep(0.01)
            except Exception as e:
                # print("ZMQServer._receive_data(): reading failed, exception: %s"%e)
                time.sleep(0.01)
                if str(e) == "Context was terminated":
                    # print("ZMQServer._receive_data(): ZMQ context was terminated,out..")
                    # self._socket.close()
                    # self._zmqContext.term()
                    self._state = ZMQServerState.STOPPED
                    # return
                # self._state==ZMQServerState.STOPPED
        # print("ZMQServer._receive_data(): out..")

    def _readEndpoints(self, configFileLocation):
        """Reads all lines from a file and returns them in a list. Newline characters are removed."""
        with open(configFileLocation, "r") as f:
            lines = f.read().splitlines()
            return lines
