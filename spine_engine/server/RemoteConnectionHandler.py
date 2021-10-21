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
import json
import os
import ast
import random 
import string
import pathlib
from spine_engine.server.util.ServerMessageParser import ServerMessageParser
from spine_engine.server.util.ServerMessage import ServerMessage
from spine_engine.server.util.FileExtractor import FileExtractor
from spine_engine.server.RemoteSpineServiceImpl import RemoteSpineServiceImpl
from spine_engine.server.util.EventDataConverter import EventDataConverter


class RemoteConnectionHandler(threading.Thread):
    """Handles one remote connection at a time from Spine Toolbox,
    executes a DAG, and returns response to the client."""

    # location, where all projects will be extracted and executed
    internalProjectFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "received_projects")

    def __init__(self,zmqConnection):
        """
        Args:
            zmqConnection: Zero-MQ connection of the client.
        """
        if not zmqConnection:
            raise ValueError("No Zero-MQ connection was provided to RemoteConnectionHandler()")
        self.zmqConn = zmqConnection
        threading.Thread.__init__(self)
        self.start()

    def run(self):
        # print("RemoteConnectionHandler.run()")
        self._execute()

    def _execute(self):
        """Executes a query with the Spine engine, and returns a response to the Zero-MQ client."""
        # debugging
        execStartTimeMs = round(time.time()*1000.0)
        # get message parts sent by the client
        msgParts=self.zmqConn.getMessageParts()
        # print("RemoteConnectionHandler._execute() Received: ")
        # print(msgParts)
        # parse JSON message
        if len(msgParts[0])>10:
            try:
                msgPart1=msgParts[0].decode("utf-8")
                # print("RemoteConnectionHandler._execute() Received JSON:\n %s"%msgPart1)
                parsedMsg=ServerMessageParser.parse(msgPart1)
                # print("parsed msg with command: %s"%parsedMsg.getCommand())

                # save attached file to the location indicated in the project_dir-field of the JSON
                data=parsedMsg.getData()
                # print("RemoteConnectionHandler._execute() data type: %s"%type(data))
                dataAsDict=json.loads(data)
                # print(f"RemoteConnectionHandler._execute(): {dataAsDict}")
                # print(f"Received msg:\n{dataAsDict}")
                # print(f"parsed project_dir: {dataAsDict['project_dir']}")
            except:
                #print("RemoteConnectionHandler._execute(): Error in parsing content, returning empty data")
                retBytes=bytes("{}", 'utf-8')
                self.zmqConn.sendReply(retBytes)
                return

            if len(parsedMsg.getFileNames()) == 1 and len(msgParts) == 2:  # check for presence of 2 message parts
                # save the file
                try:
                    # get a new local folder name based on project_dir
                    localFolder = RemoteConnectionHandler.getFolderForProject(dataAsDict['project_dir'])
                    print("RemoteConnectionHandler._execute(): using a new folder: %s"%localFolder)

                    # check for validity of the new folder
                    if not localFolder:
                        self._sendResponse(parsedMsg.getCommand(), parsedMsg.getId(),"{}")
                        return
                    # create folder, if it doesn't exist yet
                    if not os.path.exists(localFolder):
                        os.makedirs(localFolder)
                        #print("RemoteConnectionHandler._execute() Created a new folder %s"%(localFolder))
                    # print("file name: %s"%parsedMsg.getFileNames()[0])
                    f = open(os.path.join(localFolder,parsedMsg.getFileNames()[0]), "wb")
                    f.write(msgParts[1])
                    f.close()
                except Exception as e:
                    print("RemoteConnectionHandler._execute(): couldn't save the extracted file, returning empty response, reason: %s\n"%e)
                    self._sendResponse(parsedMsg.getCommand(), parsedMsg.getId(), "{}")
                    return
                try:
                    # extract the saved file
                    print(f"RemoteConnectionHandler._execute(): Extracting received "
                          f"file: {parsedMsg.getFileNames()[0]} to: {localFolder}")
                    FileExtractor.extract(os.path.join(localFolder,parsedMsg.getFileNames()[0]), localFolder)
                except Exception as e:
                    print("RemoteConnectionHandler._execute(): File extraction failed, returning empty response..")
                    self._sendResponse(parsedMsg.getCommand(), parsedMsg.getId(), "{}")
                    return
                # execute DAG in the Spine engine
                print("RemoteConnectionHandler._execute(): Executing the project")
                spineEngineImpl=RemoteSpineServiceImpl()
                #print("RemoteConnectionHandler._execute() Received data type :%s"%type(dataAsDict))
                #convertedData=self._convertTextDictToDicts(dataAsDict)
                convertedData=self._convertInput(dataAsDict,localFolder)
                #print("RemoteConnectionHandler._execute() passing data to spine engine impl: %s"%convertedData)
                eventData=spineEngineImpl.execute(convertedData)
                #print("RemoteConnectionHandler._execute(): received events/data: ")
                #print(eventData)

                # create a response message, parse and send it
                print("RemoteConnectionHandler._execute(): Execution done. Sending a response to client")
                jsonEventsData=EventDataConverter.convert(eventData)
                # print(type(jsonEventsData))
                replyMsg=ServerMessage(parsedMsg.getCommand(), parsedMsg.getId(), jsonEventsData, None)
                replyAsJson=replyMsg.toJSON()
                # print("RemoteConnectionHandler._execute() Reply to be sent: \n%s"%replyAsJson)
                replyInBytes= bytes(replyAsJson, "utf-8")
                # print("RemoteConnectionHandler._execute() Reply to be sent in bytes:%s"%replyInBytes)
                self.zmqConn.sendReply(replyInBytes)
                # self.zmqConn.close()
                # print("RemoteConnectionHandler._execute(): closed the socket to the client.")

                # delete extracted folder
                # try:
                #     time.sleep(4)
                #     FileExtractor.deleteFolder(localFolder+"/")
                #     print("RemoteConnectionHandler._execute(): Deleted folder %s"%localFolder+"/")
                # except Exception as e:
                #     print(f"RemoteConnectionHandler._execute(): Couldn't delete directory {localFolder}. Error:\n{e}")
                # debugging
                # execStopTimeMs=round(time.time()*1000.0)
                # print("RemoteConnectionHandler._execute(): duration %d ms"%(execStopTimeMs-execStartTimeMs))
            else:
                # print("RemoteConnectionHandler._execute(): no file name included, returning empty response..\n")
                self._sendResponse(parsedMsg.getCommand(),parsedMsg.getId(),"{}")

    @staticmethod
    def getFolderForProject(project_dir):
        """Returns internal folder, where the project's ZIP-file will be extracted to.

        Args:
            project_dir: project directory received from the client (remote folder)

        Returns:
            str: internal folder, empty string is returned for invalid input
        """
        if not project_dir:
            return ""
        # Convert to raw string so Windows style paths do not produce Syntax error (Unicode error). 
        # (e.g. "\U" produces an error but r\"U" does not
        fixed_project_dir = project_dir.encode("unicode_escape").decode()
        # Convert to path to OS specific PurePath and get the rightmost folder name
        if fixed_project_dir.startswith("/"):
            # It's a Posix absolute path
            p = pathlib.PurePosixPath(fixed_project_dir).stem
        else:
            # It's a Windows absolute Path
            p = pathlib.PureWindowsPath(fixed_project_dir).stem
        random_str = "".join(random.choices(string.ascii_lowercase, k=10))  # create a random string
        return os.path.join(RemoteConnectionHandler.internalProjectFolder, p + "_" + random_str)

    def _convertInput(self,inputData,localFolder):
        """Converts received input data for execution in a local folder.
        Args:
            inputData: input data as a dict.
            localFolder: local folder to be used for DAG execution
        """
        #print("RemoteConnectionHandler._convertInput(): input data")
        #print(inputData)

        #adjust project_dir to point to the local folder
        remoteFolder=inputData['project_dir']
        inputData['project_dir']=localFolder

        #loop specs
        specsKeys=inputData['specifications'].keys()
        for specKey in specsKeys:
            specItem=inputData['specifications'][specKey]
            #print("RemoteConnectionHandler._convertInput(): spec item type: %s"%type(specItem))
            i=0
            for specItemInfo in specItem:
                #print("RemoteConnectionHandler._convertInput(): spec item info type%s"%type(specItemInfo))
                #adjust definition_file_path in specs to point to the local folder
                if 'definition_file_path' in specItemInfo:
                    #print("RemoteConnectionHandler._convertInput(): spec item info contains definition_file_path")
                    originalDefinitionFilePath=specItemInfo['definition_file_path']
                    cuttedRemoteFolder=originalDefinitionFilePath.replace(remoteFolder,'')
                    modifiedDefinitionFilePath=localFolder+cuttedRemoteFolder
                    inputData['specifications'][specKey][i]['definition_file_path']=modifiedDefinitionFilePath
                #force execute_in_work-field to False
                if 'execute_in_work' in specItemInfo:
                    #print("RemoteConnectionHandler._convertInput(): spec item info contains execute_in_work")
                    inputData['specifications'][specKey][i]['execute_in_work']=False
                i+=1

        #loop items
        itemsKeys=inputData['items'].keys()
        for itemKey in itemsKeys:
            #force execute_in_work to False in items
            if 'execute_in_work' in inputData['items'][itemKey]:
                #print("RemoteConnectionHandler._convertInput() execute_in_work in an item")
                inputData['items'][itemKey]['execute_in_work']=False

        #print("RemoteConnectionHandler._convertInput(): converted data:")
        #print(inputData)

        return inputData

    def _sendResponse(self,msgCommand,msgId,data):
        replyMsg = ServerMessage(msgCommand, msgId, data, None)
        replyAsJson = replyMsg.toJSON()
        replyInBytes = bytes(replyAsJson, "utf-8")
        self.zmqConn.sendReply(replyInBytes)

    def _convertTextDictToDicts(self, data):
        newData = dict()
        # print("_convertTextDictToDicts() items: %s"%type(data['items']))
        newData['items']=ast.literal_eval(data['items'])
        newData['connections']=ast.literal_eval(data['connections'])
        newData['specifications']=ast.literal_eval(data['specifications'])
        newData['node_successors']=ast.literal_eval(data['node_successors'])
        newData['execution_permits']=ast.literal_eval(data['execution_permits'])
        newData['settings']=ast.literal_eval(data['settings'])
        newData['settings']=ast.literal_eval(data['settings'])
        newData['project_dir']=data['project_dir']
        return newData
