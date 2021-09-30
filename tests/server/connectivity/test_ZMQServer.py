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
#sys.path.append('./../../../spine_engine/server/connectivity')
import zmq
#from zmq.error import ZMQError
import time
import os

from spine_engine.server.connectivity.ZMQServer import ZMQServer
from spine_engine.server.connectivity.ZMQServerObserver import ZMQServerObserver
from spine_engine.server.connectivity.ZMQConnection import ZMQConnection
from spine_engine.server.connectivity.ZMQServer import ZMQSecurityModelState


class TestObserver(ZMQServerObserver):

    def receiveConnection(self,conn:ZMQConnection)-> None:
        #print("TestObserver.receiveConnection()")
        #parts=conn.getMessageParts()
        #print("TestObserver.receiveConnection(): parts received:")
        #print(parts)
        conn.sendReply(conn.getMessageParts()[0])



class TestZMQServer(unittest.TestCase):


    #def test_starting_stopping_server_tcp(self):
    #    """
    #    Tests the starting/stopping of the ZMQ server 
    #    """
    #    ob=TestObserver()
        #zmqServer=ZMQServer("tcp",6001,ob,ZMQSecurityModelState.STONEHOUSE,"/home/ubuntu/sw/spine/dev/zmq_server_certs/")
    #    zmqServer=ZMQServer("tcp",6001,ob,ZMQSecurityModelState.NONE,"/home/ubuntu/sw/spine/dev/zmq_server_certs/")
    #    time.sleep(1)        
    #    ret=zmqServer.close()
    #    print("test_starting_stopping_server_tcp() after close()")
    #    self.assertEqual(ret,0)


    def test_missing_secfolder(self):
        """
        Tests the starting/stopping of the ZMQ server without proper sec.folder
        """
        ob=TestObserver()
        with self.assertRaises(ValueError):
            zmqServer=ZMQServer("tcp",6002,ob,ZMQSecurityModelState.STONEHOUSE,"")
        #print("test_missing_secfolder() completed.")
    

    def test_invalid_secfolder(self):
        """
        Tests the starting/stopping of the ZMQ server without proper sec.folder
        """
        ob=TestObserver()
        with self.assertRaises(ValueError):
            zmqServer=ZMQServer("tcp",6003,ob,ZMQSecurityModelState.STONEHOUSE,"/fwhkjfnsefkjnselk")
        #print("test_invalid_secfolder() completed.")


    def test_starting_server_connection_withsecurity_tcp(self):
       """
       Tests starting of a ZMQ server with tcp, and reception of a connection and message parts (with security).
       """
       ob=TestObserver()
       zmqServer=ZMQServer("tcp",6006,ob,ZMQSecurityModelState.STONEHOUSE,"./tests/server/connectivity/secfolder")

       #connect to the server
       time.sleep(1)
       context = zmq.Context()
       socket = context.socket(zmq.REQ)

       #security configs
       #prepare folders
       #print("test_starting_server_connection_withsecurity_tcp() starting to configure security..")
       base_dir = "./tests/server/connectivity/secfolder"
       #base_dir = os.path.dirname("/home/ubuntu/sw/spine/dev/zmq_server_certs/")
       #print("test_starting_server_connection_withsecurity_tcp() dirname: %s"%base_dir)
       secret_keys_dir = os.path.join(base_dir, 'private_keys')
       keys_dir = os.path.join(base_dir, 'certificates')
       public_keys_dir = os.path.join(base_dir, 'public_keys')

       # We need two certificates, one for the client and one for
       # the server. The client must know the server's public key
       # to make a CURVE connection.

       client_secret_file = os.path.join(secret_keys_dir, "client.key_secret")
       client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
       socket.curve_secretkey = client_secret
       socket.curve_publickey = client_public

       # The client must know the server's public key to make a CURVE connection.
       server_public_file = os.path.join(public_keys_dir, "server.key")
       server_public, _ = zmq.auth.load_certificate(server_public_file)
       socket.curve_serverkey = server_public

       #print("test_starting_server_connection_withsecurity_tcp() before connect()")
       socket.connect("tcp://localhost:6006")
       #print("test_starting_server_connection_withsecurity_tcp() socket connected")
       msg_parts=[]
       part1="feiofnoknfsdnoiknsmd"
       fileArray=bytearray([1, 2, 3, 4, 5])
       part1Bytes = bytes(part1, 'utf-8')
       msg_parts.append(part1Bytes)
       msg_parts.append(fileArray)
       socket.send_multipart(msg_parts)
       #print("test_starting_server_connection_withsecurity_tcp() msg parts sent.")
       #time.sleep(1)
       msg=socket.recv()
       #print("test_starting_server_connection_withsecurity_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       socket.close()
       zmqServer.close()
       context.term()


    #def test_starting_stopping_closed_server_tcp(self):
    #    """
    #    Tests stopping of a closed ZMQ server
    #    """
     #   ob=TestObserver()
     #   zmqServer=ZMQServer("tcp",5555,ob,ZMQSecurityModelState.NONE,"")
     #   time.sleep(1)
     #   ret=zmqServer.close()
     #   self.assertEqual(ret,0)
     #   ret=zmqServer.close()
     #   self.assertEqual(ret,-1)


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
             zmqServer=ZMQServer("tcp",5550,None,ZMQSecurityModelState.NONE,"")


    def test_starting_server_connection_established_tcp(self):
       """
       Tests starting of a ZMQ server with tcp, and reception of a connection and message parts.
       """
       ob=TestObserver()
       zmqServer=ZMQServer("tcp",5556,ob,ZMQSecurityModelState.NONE,"")

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
       #print("test_starting_server_connection_established_tcp() msg parts sent.")
       #time.sleep(1)       
       msg=socket.recv()
       #print("test_starting_server_connection_established_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       socket.close()
       zmqServer.close()
       context.term()


    def test_multiple_data_items_tcp(self):
       """
       Tests transfer of multiple data items within a connection.
       """
       ob=TestObserver()
       zmqServer=ZMQServer("tcp",6000,ob,ZMQSecurityModelState.NONE,"")

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
       context.term()


    def test_multiple_sockets_tcp(self):
       """
       Tests multiple sockets over TCP.
       """
       ob=TestObserver()
       zmqServer=ZMQServer("tcp",5558,ob,ZMQSecurityModelState.NONE,"")

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
       #print("test_multiple_sockets_tcp() msg on socket 1 sent.")
       msg=socket1.recv()
       #print("test_multiple_sockets_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       socket2.send_multipart(msg_parts)
       #print("test_multiple_sockets_tcp() msg on socket 2 sent.")
       msg=socket2.recv()
       #print("test_multiple_sockets_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       socket3.send_multipart(msg_parts)
       #print("test_multiple_sockets_tcp() msg on socket 3 sent.")
       msg=socket3.recv()
       #print("test_multiple_sockets_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected

       socket1.close()
       socket2.close()
       socket3.close()
       zmqServer.close()
       context.term()


    def test_multiple_sockets_seq_transfer_tcp(self):
       """
       Tests multiple sockets over TCP.
       """

       ob=TestObserver()
       zmqServer=ZMQServer("tcp",5559,ob,ZMQSecurityModelState.NONE,"")

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
       #print("test_multiple_sockets_seq_transfer_tcp() msg on socket 1 sent.")
       socket2.send_multipart(msg_parts)
       #print("test_multiple_sockets_seq_transfer_tcp() msg on socket 2 sent.")
       socket3.send_multipart(msg_parts)
       #print("test_multiple_sockets_seq_transfer_tcp() msg on socket 3 sent.")

       msg=socket1.recv()
       #print("test_multiple_sockets_seq_transfer_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       msg=socket2.recv()
       #print("test_multiple_sockets_seq_transfer_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected
       msg=socket3.recv()
       #print("test_multiple_sockets_seq_transfer_tcp() msg received: %s"%msg)
       self.assertEqual(msg,part1Bytes) #check that echoed content is as expected

       socket1.close()
       socket2.close()
       socket3.close()
       zmqServer.close()
       context.term()
    

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

