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
#sys.path.append('./../../spine_engine/server')
#sys.path.append('./../../spine_engine/server/connectivity')
#sys.path.append('./../../spine_engine/server/util')
import zmq
import json
import time

from spine_engine.server.RemoteConnectionHandler import RemoteConnectionHandler
from spine_engine.server.connectivity.ZMQServer import ZMQServer
from spine_engine.server.connectivity.ZMQServerObserver import ZMQServerObserver
from spine_engine.server.connectivity.ZMQConnection import ZMQConnection
from spine_engine.server.util.ServerMessage import ServerMessage
from spine_engine.server.util.ServerMessageParser import ServerMessageParser
from spine_engine.server.util.EventDataConverter import EventDataConverter
from .test_RemoteConnHandlerZMQServer import RemoteConnHandlerZMQServer


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

    @classmethod
    def setUpClass(cls):
        cls._server=RemoteConnHandlerZMQServer()

    @classmethod
    def tearDownClass(cls):
        cls._server.close()

    @staticmethod
    def _dict_data(
        items, connections, node_successors,
          execution_permits,specifications,settings,
          project_dir
    ):
        """Returns a dict to be passed to the class.
        Args:
            items (list(dict)): See SpineEngine.__init()
            connections (list of dict): See SpineEngine.__init()
            node_successors (dict(str,list(str))): See SpineEngine.__init()
            execution_permits (dict(str,bool)): See SpineEngine.__init()
            specifications (dict(str,list(dict))): SpineEngine.__init()
            settings (dict): SpineEngine.__init()
            project_dir (str): SpineEngine.__init()
        Returns:
            dict
        """
        item = dict()
        item['items']=items
        item['connections']=connections
        item['node_successors']=node_successors
        item['execution_permits']=execution_permits
        item['specifications']=specifications
        item['settings']=settings
        item['project_dir']=project_dir
        return item


#    def __init__(self):
#        self.server=RemoteConnHandlerZMQServer()

    def test_init_error(self):
        with self.assertRaises(ValueError):
            handler=RemoteConnectionHandler(None)

    def test_init_complete(self):
       #connect to the server
       context = zmq.Context()
       socket = context.socket(zmq.REQ)
       socket.connect("tcp://localhost:5556")
       msg_parts=[]
       #fileArray=bytearray([1, 2, 3, 4, 5])

       dict_data2 = self._dict_data(items={'helloworld': {'type': 'Tool', 'description': '', 'x': -91.6640625,
            'y': -5.609375, 'specification': 'helloworld2', 'execute_in_work': True, 'cmd_line_args': []},
            'Data Connection 1': {'type': 'Data Connection', 'description': '', 'x': 62.7109375, 'y': 8.609375,
             'references': [{'type': 'path', 'relative': True, 'path': 'input2.txt'}]}},
            connections=[{'from': ['Data Connection 1', 'left'], 'to': ['helloworld', 'right']}],
            node_successors={'Data Connection 1': ['helloworld'], 'helloworld': []},
            execution_permits={'Data Connection 1': True, 'helloworld': True},
            project_dir = './helloworld',
            specifications = {'Tool': [{'name': 'helloworld2', 'tooltype': 'python',
            'includes': ['helloworld.py'], 'description': '', 'inputfiles': ['input2.txt'],
            'inputfiles_opt': [], 'outputfiles': [], 'cmdline_args': [], 'execute_in_work': True,
            'includes_main_path': '../../..',
            'definition_file_path':
            './helloworld/.spinetoolbox/specifications/Tool/helloworld2.json'}]},
            settings = {'appSettings/previousProject': './helloworld',
            'appSettings/recentProjectStorages': './',
            'appSettings/recentProjects': 'helloworld<>./helloworld',
            'appSettings/showExitPrompt': '2',
            'appSettings/toolbarIconOrdering':
            'Importer;;View;;Tool;;Data Connection;;Data Transformer;;Gimlet;;Exporter;;Data Store',
            'appSettings/workDir': './Spine-Toolbox/work'})
       
       #f=open('msg_data1.txt')
       #msgData = f.read()
       #f.close()
       msgDataJson=json.dumps(dict_data2)
       msgDataJson=json.dumps(msgDataJson)
       #print("test_init_complete() msg JSON-encoded data::\n%s"%msgDataJson)
       f2=open('./tests/server/test_zipfile.zip','rb')
       data = f2.read()
       f2.close()
       listFiles=["helloworld.zip"]
       msg=ServerMessage("execute","1",msgDataJson,listFiles)
       part1Bytes = bytes(msg.toJSON(), 'utf-8')
       msg_parts.append(part1Bytes)
       msg_parts.append(data)
       socket.send_multipart(msg_parts)

       time.sleep(1)
       print("test_init_complete(): listening to replies..")
       message = socket.recv()
       msgStr=message.decode('utf-8')
       #print("out recv()..Received reply (from network) %s" %msgStr)
       parsedMsg=ServerMessageParser.parse(msgStr)
       #print(parsedMsg)
       #get and decode events+data
       data=parsedMsg.getData()
       #print(type(data))
       jsonData=json.dumps(data)
       dataEvents=EventDataConverter.convertJSON(jsonData,True)       
       #print("parsed events+data, items:%d\n"%len(dataEvents))
       #self.assertEqual(len(dataEvents),34)
       self.assertEqual(dataEvents[len(dataEvents)-1][1],"COMPLETED")
       #print(dataEvents)
       #close connections
       socket.close()



    def test_loop_calls(self):
       #connect to the server
       context = zmq.Context()
       socket = context.socket(zmq.REQ)
       socket.connect("tcp://localhost:5556")
       msg_parts=[]
       #fileArray=bytearray([1, 2, 3, 4, 5])

       dict_data2 = self._dict_data(items={'helloworld': {'type': 'Tool', 'description': '', 'x': -91.6640625,
            'y': -5.609375, 'specification': 'helloworld2', 'execute_in_work': True, 'cmd_line_args': []},
            'Data Connection 1': {'type': 'Data Connection', 'description': '', 'x': 62.7109375, 'y': 8.609375,
             'references': [{'type': 'path', 'relative': True, 'path': 'input2.txt'}]}},
            connections=[{'from': ['Data Connection 1', 'left'], 'to': ['helloworld', 'right']}],
            node_successors={'Data Connection 1': ['helloworld'], 'helloworld': []},
            execution_permits={'Data Connection 1': True, 'helloworld': True},
            project_dir = './helloworld',
            specifications = {'Tool': [{'name': 'helloworld2', 'tooltype': 'python',
            'includes': ['helloworld.py'], 'description': '', 'inputfiles': ['input2.txt'],
            'inputfiles_opt': [], 'outputfiles': [], 'cmdline_args': [], 'execute_in_work': True,
            'includes_main_path': '../../..',
            'definition_file_path':
            './helloworld/.spinetoolbox/specifications/Tool/helloworld2.json'}]},
            settings = {'appSettings/previousProject': './helloworld',
            'appSettings/recentProjectStorages': './',
            'appSettings/recentProjects': 'helloworld<>./helloworld',
            'appSettings/showExitPrompt': '2',
            'appSettings/toolbarIconOrdering':
            'Importer;;View;;Tool;;Data Connection;;Data Transformer;;Gimlet;;Exporter;;Data Store',
            'appSettings/workDir': './Spine-Toolbox/work'})

       #f=open('msg_data1.txt')
       #msgData = f.read()
       #f.close()
       msgDataJson=json.dumps(dict_data2)
       msgDataJson=json.dumps(msgDataJson)
       #print("test_init_complete() msg JSON-encoded data::\n%s"%msgDataJson)
       f2=open('./tests/server/test_zipfile.zip','rb')
       data = f2.read()
       f2.close()
       listFiles=["helloworld.zip"]
       msg=ServerMessage("execute","1",msgDataJson,listFiles)
       part1Bytes = bytes(msg.toJSON(), 'utf-8')
       msg_parts.append(part1Bytes)
       msg_parts.append(data)
       i=0
       while i < 10:
           socket.send_multipart(msg_parts)
           print("test_loop_calls(): listening to replies..%d"%i)
           message = socket.recv()
           msgStr=message.decode('utf-8')
           #print("out recv()..Received reply %s" %msgStr)
           parsedMsg=ServerMessageParser.parse(msgStr)
           #get and decode events+data
           data=parsedMsg.getData()
           #print(type(data))
           jsonData=json.dumps(data)
           dataEvents=EventDataConverter.convertJSON(jsonData,True)
           #print("parsed events+data, items:%d\n"%len(dataEvents))
           #self.assertEqual(len(dataEvents),34)
           self.assertEqual(dataEvents[len(dataEvents)-1][1],"COMPLETED")
           #print(dataEvents)
           i+=1
       #close connections
       socket.close()


    def test_init_no_binarydata(self):
       """
       Send message with JSON, but no binary data.
       """
       #connect to the server
       context = zmq.Context()
       socket = context.socket(zmq.REQ)
       socket.connect("tcp://localhost:5556")
       msg_parts=[]

       f=open('./tests/server/msg_data1.txt')
       msgData = f.read()
       f.close()
       msgDataJson=json.dumps(msgData)
       listFiles=["helloworld.zip"]
       msg=ServerMessage("execute","1",msgDataJson,listFiles)
       part1Bytes = bytes(msg.toJSON(), 'utf-8')
       msg_parts.append(part1Bytes)
       socket.send_multipart(msg_parts)

       #time.sleep(1)
       print("listening to replies..")
       message = socket.recv()
       msgStr=message.decode('utf-8')
       #print("out recv()..Received reply %s" %msgStr)
       parsedMsg=ServerMessageParser.parse(msgStr)
       data=parsedMsg.getData()
       print("received data: %s"%data)
       self.assertEqual(str(data),"{}")


    def test_no_filename(self):
       #connect to the server
       context = zmq.Context()
       socket = context.socket(zmq.REQ)
       socket.connect("tcp://localhost:5556")
       msg_parts=[]
       #fileArray=bytearray([1, 2, 3, 4, 5])

       f=open('./tests/server/msg_data1.txt')
       msgData = f.read()
       f.close()
       msgDataJson=json.dumps(msgData)
       #print("test_init_complete() msg JSON-encoded data::\n%s"%msgDataJson)
       f2=open('./tests/server/test_zipfile.zip','rb')
       data = f2.read()
       f2.close()
       msg=ServerMessage("execute","1",msgDataJson,None)
       part1Bytes = bytes(msg.toJSON(), 'utf-8')
       msg_parts.append(part1Bytes)
       msg_parts.append(data)
       socket.send_multipart(msg_parts)

       print("listening to replies..")
       message = socket.recv()
       msgStr=message.decode('utf-8')
       #print("out recv()..Received reply %s" %msgStr)
       parsedMsg=ServerMessageParser.parse(msgStr)
       #print(type(parsedMsg))
       #get and decode events+data
       data=parsedMsg.getData()
       self.assertEqual(str(data),"{}")

       #close connections
       socket.close()


    def test_invalid_json(self):
       #connect to the server
       context = zmq.Context()
       socket = context.socket(zmq.REQ)
       socket.connect("tcp://localhost:5556")
       msg_parts=[]

       f=open('./tests/server/msg_data2.txt')
       msgData = f.read()
       f.close()
       #print("test_init_complete() msg JSON-encoded data::\n%s"%msgDataJson)
       f2=open('./tests/server/test_zipfile.zip','rb')
       data = f2.read()
       f2.close()
       msg=ServerMessage("execute","1",msgData,None)
       part1Bytes = bytes(msg.toJSON(), 'utf-8')
       msg_parts.append(part1Bytes)
       msg_parts.append(data)
       socket.send_multipart(msg_parts)

       print("listening to replies..")
       message = socket.recv()
       msgStr=message.decode('utf-8')
       print("out recv()..Received reply %s" %msgStr)
       self.assertEqual(msgStr,"{}")

       #close connections
       socket.close()


if __name__ == '__main__':
    #server=RemoteConnHandlerZMQServer()
    unittest.main()
    #server.close()

