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
Unit tests for RemoteConnHandlerZMQServer class.
:author: P. Pääkkönen (VTT)
:date:   30.8.2021
"""

import unittest
from spine_engine.server.remote_connection_handler import RemoteConnectionHandler
from spine_engine.server.connectivity.zmq_server import ZMQServer, ZMQSecurityModelState
from spine_engine.server.connectivity.zmq_server_observer import ZMQServerObserver
from spine_engine.server.connectivity.zmq_connection import ZMQConnection


class TestObserver(ZMQServerObserver):
    def __init__(self):
        pass

    def receiveConnection(self, conn: ZMQConnection) -> None:
        # print("TestObserver.receiveConnection()")
        # parts=conn.get_message_parts()
        # print("TestObserver.receiveConnection(): parts received:")
        # print(parts)
        # conn.send_reply(conn.get_message_parts()[0])
        self.conn = conn
        connHandler = RemoteConnectionHandler(conn)
        # print("TestObserver.receiveConnection() RemoteConnectionHandler started.")

    def getConnection(self):
        return self.conn


class RemoteConnHandlerZMQServer:
    def __init__(self):
        self.ob = TestObserver()
        # self.zmqServer=ZMQServer("tcp",5556,self.ob,ZMQSecurityModelState.STONEHOUSE,"./tests/server/connectivity/secfolder")
        self.zmqServer = ZMQServer("tcp", 5556, self.ob, ZMQSecurityModelState.NONE, "")

    def close(self):
        self.zmqServer.close()


# server=RemoteConnHandlerZMQServer()
