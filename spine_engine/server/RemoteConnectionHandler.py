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

import time
import threading
import sys
import json
import os
import ast
import random 
import string
from pathlib import Path

#sys.path.append('./util')
from spine_engine.server.util.ServerMessageParser import ServerMessageParser
from spine_engine.server.util.ServerMessage import ServerMessage
from spine_engine.server.util.FileExtractor import FileExtractor
from spine_engine.server.RemoteSpineServiceImpl import RemoteSpineServiceImpl
from spine_engine.server.util.EventDataConverter import EventDataConverter

class RemoteConnectionHandler(threading.Thread):

    """
    Handles one remote connection at a time from Spine Toolbox, executes a DAG, and returns 
    response to the client.
    """

    internalProjectFolder="./received_projects/" #location, where all projects will be extracted and executed


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
        #print("RemoteConnectionHandler.run()")
        self._execute()
        


    def _execute(self):
        """
        Executes a query with the Spine engine, and returns a response to the Zero-MQ client.
        """
        #debugging
        execStartTimeMs=round(time.time()*1000.0)

        #get message parts sent by the client
        msgParts=self.zmqConn.getMessageParts()
        #print("RemoteConnectionHandler._execute() Received: ")
        #print(msgParts)

        #parse JSON message 
        if len(msgParts[0])>10:
            try:
                msgPart1=msgParts[0].decode("utf-8")
                #print("RemoteConnectionHandler._execute() Received JSON:\n %s"%msgPart1)
                parsedMsg=ServerMessageParser.parse(msgPart1)
                #print("parsed msg with command: %s"%parsedMsg.getCommand()) 

                #save attached file to the location indicated in the project_dir-field of the JSON
                data=parsedMsg.getData()
                #print("RemoteConnectionHandler._execute() data type: %s"%type(data))
                dataAsDict=json.loads(data)
                #print(dataAsDict)
                #print("parsed data from the received msg: %s"%data)
                #print("parsed project_dir: %s"%data['project_dir'])
            except:
                #print("RemoteConnectionHandler._execute(:) Error in parsing content, returning empty data")
                retBytes=bytes("{}", 'utf-8')
                self.zmqConn.sendReply(retBytes)
                return 

            if(len(parsedMsg.getFileNames())==1 and len(msgParts)==2): #check for presence of 2 message parts

                #save the file
                try:
                    #get a new local folder name based on project_dir
                    localFolder=RemoteConnectionHandler.getFolderForProject(dataAsDict['project_dir'])
                    #print("RemoteConnectionHandler._execute(): using a new folder: %s"%localFolder)

                    #check for validity of the new folder
                    if len(localFolder)==0:
                        #print("RemoteConnectionHandler._execute() Couldn't parse a valid local folder based on project_dir")
                        self._sendResponse(parsedMsg.getCommand(),parsedMsg.getId(),"{}")
                        return

                    #create folder, if it doesn't exist yet
                    if os.path.exists(localFolder+"/")==False:
                        os.makedirs(localFolder+"/")
                        #print("RemoteConnectionHandler._execute() Created a new folder %s"%(localFolder+"/"))

                    #print("file name: %s"%parsedMsg.getFileNames()[0])
                    f=open(localFolder+"/"+parsedMsg.getFileNames()[0], "wb")
                    f.write(msgParts[1])
                    f.close()
                    #print("saved received file: %s to folder: %s"%(parsedMsg.getFileNames()[0],localFolder))
                except exp as e:
                    #print("RemoteConnectionHandler._execute(): couldn't save the extracted file, returning empty response, reason: %s\n"%e)
                    self._sendResponse(parsedMsg.getCommand(),parsedMsg.getId(),"{}")
                    return
                try:
                    #extract the saved file
                    FileExtractor.extract(localFolder+"/"+parsedMsg.getFileNames()[0],localFolder+"/")
                    #print("extracted file: %s to folder: %s"%(parsedMsg.getFileNames()[0],dataAsDict['project_dir']))
                except:
                    #print("RemoteConnectionHandler._execute(): couldn't extract the file, returning empty response..\n")
                    self._sendResponse(parsedMsg.getCommand(),parsedMsg.getId(),"{}")
                    return
                #execute DAG in the Spine engine
                spineEngineImpl=RemoteSpineServiceImpl()
                #print("RemoteConnectionHandler._execute() Received data type :%s"%type(dataAsDict))
                #convertedData=self._convertTextDictToDicts(dataAsDict)
                convertedData=self._convertInput(dataAsDict,localFolder)
                #print("RemoteConnectionHandler._execute() passing data to spine engine impl: %s"%convertedData)
                eventData=spineEngineImpl.execute(convertedData)
                #print("RemoteConnectionHandler._execute(): received events/data: ")
                #print(eventData)

                #delete extracted folder
                #FileExtractor.deleteFolder(dataAsDict['project_dir']+"/")
                #print("RemoteConnectionHandler._execute(): Deleted folder %s"%dataAsDict['project_dir']+"/")

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

                #delete extracted folder
                try:
                    time.sleep(4)
                    FileExtractor.deleteFolder(localFolder+"/")
                    #print("RemoteConnectionHandler._execute(): Deleted folder %s"%localFolder+"/")
                    localFolderParent=Path(localFolder+"/").parent
                    #print("RemoteConnectionHandler._execute(): local folder parent path: %s"%localFolderParent)
                    #print(str(localFolderParent))
                    #delete also the parent path
                    FileExtractor.deleteFolder("./"+str(localFolderParent)+"/")
                    #print("RemoteConnectionHandler._execute(): Deleted parent folder ./%s"%localFolderParent)
                except:                 
                    pass
                    #print("RemoteConnectionHandler._execute(): folder %s wax probably already deleted, returning.."%dataAsDict)
       
                #debugging
                #execStopTimeMs=round(time.time()*1000.0)
                #print("RemoteConnectionHandler._execute(): duration %d ms"%(execStopTimeMs-execStartTimeMs))

            
            else:
                #print("RemoteConnectionHandler._execute(): no file name included, returning empty response..\n")
                self._sendResponse(parsedMsg.getCommand(),parsedMsg.getId(),"{}")
             


    @staticmethod
    def getFolderForProject(projectDir):
        """
        Returns internal folder, where the project's ZIP-file will be extracted to.
        Args:    
            projectDir: project directory received from the client (remote folder)
        Returns:
            internal folder, empty string is returned for invalid input
        """
        if projectDir==None:
            return ""
        if len(projectDir)==0:
            return ""

        retStr=""
        projectFolderName=""
        #get rightmost folder of the project directory (strip the remote folder path)
        slashIndex=projectDir.rfind('/')
        if slashIndex!=-1:
            projectFolderName=projectDir[slashIndex:]
        else:
            projectFolderName=projectDir
        #print("RemoteConnectionHandler._getFolderForProject() Project folder name: %s"%projectFolderName)
        #create a random string
        randomStr=''.join(random.choices(string.ascii_lowercase, k = 10)) 
        #print("RemoteConnectionHandler._getFolderForProject() random str: %s"%randomStr)
        
        retStr=RemoteConnectionHandler.internalProjectFolder+randomStr+projectFolderName

        return retStr


    def _convertInput(self,inputData,localFolder):
        """
        Converts received input data for execution in a local folder.
        Args:
            inputData: input data as a dict.
        """
        #adjust project_dir
        remoteFolder=inputData['project_dir']
        inputData['project_dir']=localFolder
        #specsDict=inputData['specifications']
        #for item in specsDict:
        #    print(type(item))
        #    print(item)
        #print(type(inputData['specifications']['Tool'][0]))

        #adjust definition_file_path in specs to point to the local folder
        originalDefinitionFilePath=inputData['specifications']['Tool'][0]['definition_file_path']
        #print("RemoteConnectionHandler._convertInput() original definition file path: %s"%originalDefinitionFilePath)
        #cut original path
        cuttedRemoteFolder=originalDefinitionFilePath.replace(remoteFolder,'')
        #print("RemoteConnectionHandler._convertInput() cutted remote folder: %s"%cuttedRemoteFolder)
        modifiedDefinitionFilePath=localFolder+cuttedRemoteFolder
        #print("RemoteConnectionHandler._convertInput() modified definition file path: %s"%modifiedDefinitionFilePath)
        inputData['specifications']['Tool'][0]['definition_file_path']=modifiedDefinitionFilePath
        #print(inputData['specifications']['Tool'][0]['definition_file_path'])

        return inputData


    def _sendResponse(self,msgCommand,msgId,data):
        """
        """
        replyMsg=ServerMessage(msgCommand,msgId,data,None)
        replyAsJson=replyMsg.toJSON()
        replyInBytes= bytes(replyAsJson, 'utf-8')
        self.zmqConn.sendReply(replyInBytes)



    def _convertTextDictToDicts(self,data):
        newData=dict()
        #print("_convertTextDictToDicts() items: %s"%type(data['items']))
        newData['items']=ast.literal_eval(data['items'])
        newData['connections']=ast.literal_eval(data['connections'])
        newData['specifications']=ast.literal_eval(data['specifications'])
        newData['node_successors']=ast.literal_eval(data['node_successors'])
        newData['execution_permits']=ast.literal_eval(data['execution_permits'])
        newData['settings']=ast.literal_eval(data['settings'])
        newData['settings']=ast.literal_eval(data['settings'])
        newData['project_dir']=data['project_dir']
        return newData
