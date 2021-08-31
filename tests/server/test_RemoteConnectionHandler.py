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
Unit tests for RemoteConnectionHandler class.
:author: P. Pääkkönen (VTT)
:date:   24.8.2021
"""

import unittest
from unittest.mock import NonCallableMagicMock

import sys
sys.path.append('./../../spine_engine/server')
sys.path.append('./../../spine_engine/server/connectivity')
sys.path.append('./../../spine_engine/server/util')
import zmq
import json
import time

from RemoteConnectionHandler import RemoteConnectionHandler
from ZMQServer import ZMQServer
from ZMQServerObserver import ZMQServerObserver
from ZMQConnection import ZMQConnection
from ServerMessage import ServerMessage
from ServerMessageParser import ServerMessageParser
from EventDataConverter import EventDataConverter


class TestObserver(ZMQServerObserver):

#    def __init__():
#           

    def receiveConnection(self,conn:ZMQConnection)-> None:
        print("TestObserver.receiveConnection()")
        #parts=conn.getMessageParts()
        #print("TestObserver.receiveConnection(): parts received:")
        #print(parts)
        conn.sendReply(conn.getMessageParts()[0])
        self.conn=conn

    def getConnection(self):
        return self.conn



class TestRemoteConnectionHandler(unittest.TestCase):


    def test_init_error(self):
        with self.assertRaises(ValueError):
            handler=RemoteConnectionHandler(None)

    def test_init_complete(self):
       #ob=TestObserver()
       #zmqServer=ZMQServer("tcp",5556,ob)

       #connect to the server
       context = zmq.Context()
       socket = context.socket(zmq.REQ)
       socket.connect("tcp://localhost:5556")
       msg_parts=[]
       #fileArray=bytearray([1, 2, 3, 4, 5])
       
       f=open('msg_data1.txt')
       msgData = f.read()
       f.close()
       msgDataJson=json.dumps(msgData)
       #print("test_init_complete() msg JSON-encoded data::\n%s"%msgDataJson)
       f2=open('test_zipfile.zip','rb')
       data = f2.read()
       f2.close()
       listFiles=["helloworld.zip"]
       msg=ServerMessage("execute","1",msgDataJson,listFiles)
       part1Bytes = bytes(msg.toJSON(), 'utf-8')
       msg_parts.append(part1Bytes)
       msg_parts.append(data)
       socket.send_multipart(msg_parts)

       time.sleep(1)
       #conn=ob.getConnection()
       #print("received connection: ")
       #print(conn)
       #pass the connection to the connection handler
       #connHandler=RemoteConnectionHandler(conn)
       print("listening to replies..")
       message = socket.recv()
       msgStr=message.decode('utf-8')
       print("out recv()..Received reply %s" %msgStr)
       parsedMsg=ServerMessageParser.parse(msgStr)
       #print(type(parsedMsg))
       #get and decode events+data
       data=parsedMsg.getData()
       #print(type(data))
       jsonData=json.dumps(data)
       dataEvents=EventDataConverter.convertJSON(jsonData,True)       
       print("parsed events+data, items:%d\n"%len(dataEvents))
       self.assertEqual(len(dataEvents),31)
       #print(dataEvents)
       #close connections
       #socket.close()
       #context.term()
       #zmqServer.close()


    def test_init_no_binarydata(self):
       """
       Send message with JSON, but no binary data.
       """
       #connect to the server       
       context = zmq.Context()
       socket = context.socket(zmq.REQ)
       socket.connect("tcp://localhost:5556")
       msg_parts=[]

       f=open('msg_data1.txt')
       msgData = f.read()
       f.close()
       msgDataJson=json.dumps(msgData)
       listFiles=["helloworld.zip"]
       msg=ServerMessage("execute","1",msgDataJson,listFiles)
       part1Bytes = bytes(msg.toJSON(), 'utf-8')
       msg_parts.append(part1Bytes)
       socket.send_multipart(msg_parts)

       time.sleep(1)
       print("listening to replies..")
       message = socket.recv()
       msgStr=message.decode('utf-8')
       print("out recv()..Received reply %s" %msgStr)


if __name__ == '__main__':
    unittest.main()

