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

import unittest
from unittest.mock import NonCallableMagicMock

import sys
sys.path.append('./../../../spine_engine/server/connectivity')
import zmq
#from zmq.error import ZMQError
import time

from ZMQServer import ZMQServer
from ZMQServerObserver import ZMQServerObserver
from ZMQConnection import ZMQConnection


class TestObserver(ZMQServerObserver):

    def receiveConnection(self,conn:ZMQConnection)-> None:
        #print("TestObserver.receiveConnection()")
        #parts=conn.getMessageParts()
        #print("TestObserver.receiveConnection(): parts received:")
        #print(parts)
        conn.sendReply(conn.getMessageParts()[0])



class TestZMQServer(unittest.TestCase):


    def test_starting_stopping_server_tcp(self):
        """
        Tests the starting/stopping of the ZMQ server 
        """
        ob=TestObserver()
        zmqServer=ZMQServer("tcp",5555,ob)
        ret=zmqServer.close()
        self.assertEqual(ret,0)


    def test_starting_stopping_closed_server_tcp(self):
        """
        Tests stopping of a closed ZMQ server
        """
        ob=TestObserver()
        zmqServer=ZMQServer("tcp",5555,ob)
        ret=zmqServer.close()
        self.assertEqual(ret,0)
        ret=zmqServer.close()
        self.assertEqual(ret,-1)


#    def test_starting_duplicate_server_tcp(self):
#        """
#        Tests starting duplicate ZMQ servers with tcp
#        """
#        ob=TestObserver()
#        zmqServer=ZMQServer("tcp",5555,ob)
#        with self.assertRaises(ValueError):
#            zmqServer2=ZMQServer("tcp",5555,ob)
#        ret=zmqServer.close()
#        self.assertEqual(ret,0)
#        time.sleep(1)


    def test_starting_server_noobserver_tcp(self):
         """
         Tests starting ZMQ server without observer.
         """
         with self.assertRaises(ValueError):
             zmqServer=ZMQServer("tcp",5555,None)


    def test_starting_server_connection_established_tcp(self):
       """
       Tests starting of a ZMQ server with tcp, and reception of a connection and message parts.
       """
       ob=TestObserver()
       zmqServer=ZMQServer("tcp",5556,ob)

       #connect to the server
       context = zmq.Context()
       socket = context.socket(zmq.REQ)
       socket.connect("tcp://localhost:5556")
       msg_parts=[]
       part1="feiofnoknfsdnoiknsmd"
       fileArray=bytearray([1, 2, 3, 4, 5]) 
       part1Bytes = bytes(part1, 'utf-8') 
       msg_parts.append(part1Bytes)
       msg_parts.append(fileArray)
       socket.send_multipart(msg_parts)
       print("test_starting_server_connection_established_tcp() msg parts sent.")
       #time.sleep(1)       
       msg=socket.recv()
       print("test_starting_server_connection_established_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       socket.close()
       zmqServer.close()


    def test_multiple_data_items_tcp(self):
       """
       Tests transfer of multiple data items within a connection.
       """
       ob=TestObserver()
       zmqServer=ZMQServer("tcp",6000,ob)

       #connect to the server
       context = zmq.Context()
       socket = context.socket(zmq.REQ)
       socket.connect("tcp://localhost:6000")
       msg_parts=[]
       part1="feiofnoknfsdnoiknsmd"
       fileArray=bytearray([1, 2, 3, 4, 5])
       part1Bytes = bytes(part1, 'utf-8')
       msg_parts.append(part1Bytes)
       msg_parts.append(fileArray)
       j=0
       while j < 100:           
           socket.send_multipart(msg_parts)
           #print("test_multiple_data_items_tcp() msg part %d sent."%j)
           msg=socket.recv()
           #print("test_multiple_data_items_tcp() msg received: %s"%msg)
           self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
           j=j+1
       socket.close()
       zmqServer.close()


    def test_multiple_sockets_tcp(self):
       """
       Tests multiple sockets over TCP.
       """
       ob=TestObserver()
       zmqServer=ZMQServer("tcp",5558,ob)

       #connect to the server
       context = zmq.Context()
       socket1 = context.socket(zmq.REQ)
       socket2 = context.socket(zmq.REQ)
       socket3 = context.socket(zmq.REQ)
       socket1.connect("tcp://localhost:5558")
       socket2.connect("tcp://localhost:5558")
       socket3.connect("tcp://localhost:5558")
       msg_parts=[]
       part1="feiofnoknfsdnoiknsmd"
       fileArray=bytearray([1, 2, 3, 4, 5])
       part1Bytes = bytes(part1, 'utf-8')
       msg_parts.append(part1Bytes)
       msg_parts.append(fileArray)
       #send over multiple sockets
       socket1.send_multipart(msg_parts)
       print("test_multiple_sockets_tcp() msg on socket 1 sent.")
       msg=socket1.recv()
       print("test_multiple_sockets_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       socket2.send_multipart(msg_parts)
       print("test_multiple_sockets_tcp() msg on socket 2 sent.")
       msg=socket2.recv()
       print("test_multiple_sockets_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       socket3.send_multipart(msg_parts)
       print("test_multiple_sockets_tcp() msg on socket 3 sent.")
       msg=socket3.recv()
       print("test_multiple_sockets_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected

       socket1.close()
       socket2.close()
       socket3.close()
       zmqServer.close()


    def test_multiple_sockets_seq_transfer_tcp(self):
       """
       Tests multiple sockets over TCP.
       """

       ob=TestObserver()
       zmqServer=ZMQServer("tcp",5559,ob)

       #connect to the server
       context = zmq.Context()
       socket1 = context.socket(zmq.REQ)
       socket2 = context.socket(zmq.REQ)
       socket3 = context.socket(zmq.REQ)
       socket1.connect("tcp://localhost:5559")
       socket2.connect("tcp://localhost:5559")
       socket3.connect("tcp://localhost:5559")
       msg_parts=[]
       part1="feiofnoknfsdnoiknsmd"
       fileArray=bytearray([1, 2, 3, 4, 5])
       part1Bytes = bytes(part1, 'utf-8')
       msg_parts.append(part1Bytes)
       msg_parts.append(fileArray)
       #send over multiple sockets
       socket1.send_multipart(msg_parts)
       print("test_multiple_sockets_seq_transfer_tcp() msg on socket 1 sent.")
       socket2.send_multipart(msg_parts)
       print("test_multiple_sockets_seq_transfer_tcp() msg on socket 2 sent.")
       socket3.send_multipart(msg_parts)
       print("test_multiple_sockets_seq_transfer_tcp() msg on socket 3 sent.")

       msg=socket1.recv()
       print("test_multiple_sockets_seq_transfer_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       msg=socket2.recv()
       print("test_multiple_sockets_seq_transfer_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       msg=socket3.recv()
       print("test_multiple_sockets_seq_transfer_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected

       socket1.close()
       socket2.close()
       socket3.close()
       zmqServer.close()

    

#    def test_multiple_data_items_sequentially_tcp(self):
#       """
#       Tests transfer of multiple data items sequentially within a connection (fails).
#       """
#       ob=TestObserver()
#       zmqServer=ZMQServer("tcp",5557,ob)

       #connect to the server
#       context = zmq.Context()
#       socket = context.socket(zmq.REQ)
#       socket.connect("tcp://localhost:5557")
#       msg_parts=[]
#       part1="feiofnoknfsdnoiknsmd"
#       fileArray=bytearray([1, 2, 3, 4, 5])
#       part1Bytes = bytes(part1, 'utf-8')
#       msg_parts.append(part1Bytes)
#       msg_parts.append(fileArray)
#       socket.send_multipart(msg_parts)
#       with self.assertRaises(zmq.ZMQError):
#           socket.send_multipart(msg_parts) 
#       socket.close()
#       zmqServer.close()



if __name__ == '__main__':
    unittest.main()

