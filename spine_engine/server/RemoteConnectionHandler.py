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
Contains RemoteConnectionHandler class for receiving control messages(with project content) from the Toolbox,
and running a DAG with Spine Engine. Only one DAG can be executed at a time.
:authors: P. Pääkkönen (VTT)
:date:   24.08.2021
"""

import threading
import sys
import json
import ast
sys.path.append('./util')
from ServerMessageParser import ServerMessageParser
from ServerMessage import ServerMessage
from FileExtractor import FileExtractor
from RemoteSpineServiceImpl import RemoteSpineServiceImpl
from EventDataConverter import EventDataConverter

class RemoteConnectionHandler(threading.Thread):

    """
    Handles one remote connection at a time from Spine Toolbox, executes a DAG, and returns 
    response to the client.
    """


    def __init__(
        self,zmqConnection
    ):
        """
        Args:
            zmqConnection: Zero-MQ connection of the client.
        """
        if zmqConnection==None:
            raise ValueError("No Zero-MQ connection was provided to RemoteConnectionHandler()")
        self.zmqConn=zmqConnection
        threading.Thread.__init__(self)
        self.start()


    def run(self):
        print("RemoteConnectionHandler.run()")
        self._execute()
        


    def _execute(self):
        """
        Executes a query with the Spine engine, and returns a response to the Zero-MQ client.
        """
        #get message parts sent by the client
        msgParts=self.zmqConn.getMessageParts()
        #print("RemoteConnectionHandler._execute() Received: ")
        #print(msgParts)

        #parse JSON message 
        if len(msgParts[0])>10:
            msgPart1=msgParts[0].decode("utf-8")
            print("RemoteConnectionHandler._execute() Received JSON:\n %s"%msgPart1)
            parsedMsg=ServerMessageParser.parse(msgPart1)
            print("parsed msg with command: %s"%parsedMsg.getCommand()) 

            #save attached file to the location indicated in the project_dir-field of the JSON
            data=parsedMsg.getData()
            #print("RemoteConnectionHandler._execute() data type: %s"%type(data))
            dataAsDict=json.loads(data)
            #print(dataAsDict)
            #print("parsed data from the received msg: %s"%data)
            #print("parsed project_dir: %s"%data['project_dir'])

            if(len(parsedMsg.getFileNames())==1):

                #save the file
                try:
                    print("file name: %s"%parsedMsg.getFileNames()[0])
                    f=open(dataAsDict['project_dir']+"/"+parsedMsg.getFileNames()[0], "wb")
                    f.write(msgParts[1])
                    f.close()
                    print("saved received file: %s to folder: %s"%(parsedMsg.getFileNames()[0],dataAsDict['project_dir']))
                except:
                    print("couldn't save the file, returning empty response..\n")
                #extract the saved file
                FileExtractor.extract(dataAsDict['project_dir']+"/"+parsedMsg.getFileNames()[0],dataAsDict['project_dir']+"/")
                print("extracted file: %s to folder: %s"%(parsedMsg.getFileNames()[0],dataAsDict['project_dir']))

                #execute DAG in the Spine engine
                spineEngineImpl=RemoteSpineServiceImpl()
                convertedData=self._convertTextDictToDicts(dataAsDict)
                #print("RemoteConnectionHandler._execute() passing data to spine engine: %s"%convertedData)
                eventData=spineEngineImpl.execute(convertedData)
                #print("received events/data: ")
                #print(eventData)

                #create a response message,parse and send it
                jsonEventsData=EventDataConverter.convert(eventData)
                #print(type(jsonEventsData))
                replyMsg=ServerMessage(parsedMsg.getCommand(),parsedMsg.getId(),jsonEventsData,None)
                replyAsJson=replyMsg.toJSON()
                #print("RemoteConnectionHandler._execute() Reply to be sent: \n%s"%replyAsJson)
                replyInBytes= bytes(replyAsJson, 'utf-8')
                #print("RemoteConnectionHandler._execute() Reply to be sent in bytes:%s"%replyInBytes)
                self.zmqConn.sendReply(replyInBytes)
                #self.zmqConn.close()
                #print("RemoteConnectionHandler._execute(): closed the socket to the client.")
                


    def _convertTextDictToDicts(self,data):
        newData=dict()
        newData['items']=ast.literal_eval(data['items'])
        newData['connections']=ast.literal_eval(data['connections'])
        newData['specifications']=ast.literal_eval(data['specifications'])
        newData['node_successors']=ast.literal_eval(data['node_successors'])
        newData['execution_permits']=ast.literal_eval(data['execution_permits'])
        newData['settings']=ast.literal_eval(data['settings'])
        newData['settings']=ast.literal_eval(data['settings'])
        newData['project_dir']=data['project_dir']
        return newData
