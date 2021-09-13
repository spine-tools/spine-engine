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

import sys
sys.path.append('./../../spine_engine/server')
sys.path.append('./../../spine_engine/server/connectivity')
sys.path.append('./../../spine_engine/server/util')
import zmq

from RemoteConnectionHandler import RemoteConnectionHandler
from ZMQServer import ZMQServer
from ZMQServerObserver import ZMQServerObserver
from ZMQConnection import ZMQConnection


class TestObserver(ZMQServerObserver):

    def __init__(self):
        pass


    def receiveConnection(self,conn:ZMQConnection)-> None:
        print("TestObserver.receiveConnection()")
        #parts=conn.getMessageParts()
        #print("TestObserver.receiveConnection(): parts received:")
        #print(parts)
        #conn.sendReply(conn.getMessageParts()[0])
        self.conn=conn
        connHandler=RemoteConnectionHandler(conn)
        print("TestObserver.receiveConnection() RemoteConnectionHandler started.")

    def getConnection(self):
        return self.conn



class RemoteConnHandlerZMQServer:

    def __init__(self):
        self.ob=TestObserver()
        self.zmqServer=ZMQServer("tcp",5556,self.ob)

    def close(self):
        self.zmqServer.close()


#server=RemoteConnHandlerZMQServer()


