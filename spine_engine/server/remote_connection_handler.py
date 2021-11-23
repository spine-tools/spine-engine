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
import os
import ast
import pathlib
import uuid
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.util.file_extractor import FileExtractor
from spine_engine.server.remote_spine_service_impl import RemoteSpineServiceImpl
from spine_engine.server.util.event_data_converter import EventDataConverter


class RemoteConnectionHandler(threading.Thread):
    """Handles one remote connection at a time from Spine Toolbox,
    executes a DAG, and returns response to the client."""

    # location, where all projects will be extracted and executed
    internalProjectFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "received_projects")

    def __init__(self, zmqConnection, server_msg, zip_file):
        """
        Args:
            zmqConnection: Zero-MQ connection of the client.
        """
        if not zmqConnection:
            raise ValueError("No Zero-MQ connection was provided to RemoteConnectionHandler()")
        self.zmqConn = zmqConnection
        threading.Thread.__init__(self, target=self._execute, args=(server_msg, zip_file,))
        self.start()

    def _execute(self, server_msg, zip_file):
        """Executes a query with the Spine engine, and returns a response to the Zero-MQ client.

        Args:
            server_msg (ServerMessage): Parsed ServerMessage
            zip_file (bytes): zip-file
        """
        # execStartTimeMs = round(time.time() * 1000.0)
        # Parse JSON message
        cmd = server_msg.getCommand()
        msg_id = server_msg.getId()
        msg_data = server_msg.getData()
        file_names = server_msg.getFileNames()
        if not len(file_names) == 1:  # No file name included
            print("Received msg contained no file name for the zip-file.")
            self.zmqConn.send_error_reply(cmd, msg_id, "Zip-file name missing")
            return
        if not msg_data["project_dir"]:
            print("Key project_dir missing from received msg. Can not create a local project directory.")
            self.zmqConn.send_error_reply(cmd, msg_id, "Problem in execute request. Key 'project_dir' was "
                                                       "None or an empty string.")
            return
        # Solve a new local directory name based on project_dir
        local_project_dir = self.path_for_local_project_dir(msg_data["project_dir"])
        # Create project directory
        try:
            os.makedirs(local_project_dir)
        except OSError:
            print(f"Creating project directory '{local_project_dir}' failed")
            self.zmqConn.send_error_reply(cmd, msg_id, f"Server failed in creating a project "
                                                       f"directory for the received project '{local_project_dir}'")
            return
        # Save the received zip file
        save_file_path = os.path.join(local_project_dir, file_names[0])
        try:
            with open(os.path.join(local_project_dir, file_names[0]), "wb") as f:
                f.write(zip_file)
        except Exception as e:
            print(f"Saving the received file to '{save_file_path}' failed. [{type(e).__name__}: {e}")
            self.zmqConn.send_error_reply(cmd, msg_id, f"Server failed in saving the received file "
                                                       f"to '{save_file_path}' ({type(e).__name__} at server)")
            return
        # Extract the saved file
        print(f"Extracting received file: {file_names[0]} to: {local_project_dir}")
        try:
            FileExtractor.extract(os.path.join(local_project_dir, file_names[0]), local_project_dir)
        except Exception as e:
            print(f"File extraction failed: {type(e).__name__}: {e}")
            self.zmqConn.send_error_reply(cmd, msg_id, f"{type(e).__name__}: {e}. - File extraction failed on Server")
            return
        # Execute DAG in the Spine engine
        print("Executing the project")
        spineEngineImpl = RemoteSpineServiceImpl()
        # print("RemoteConnectionHandler._execute() Received data type :%s"%type(dataAsDict))
        # convertedData=self._convertTextDictToDicts(dataAsDict)
        converted_data = self.convert_input(msg_data, local_project_dir)
        # print("RemoteConnectionHandler._execute() passing data to spine engine impl: %s"%convertedData)
        event_data = spineEngineImpl.execute(converted_data)
        # NOTE! All execution event messages generated while running the DAG are collected into a single list.
        # This list is sent back to Toolbox in a single message only after the whole DAG has finished execution
        # in a single message. This means that Spine Toolbox cannot update the GUI (e.g. animations in Design
        # View don't work, and the execution progress is not updated to Item Execution Log (or other Logs)
        # until all items have finished.

        # Create a response message, send it
        print("Execution done. Sending a response to client")
        json_events_data = EventDataConverter.convert(event_data)
        self.zmqConn.send_response(cmd, msg_id, json_events_data)
        # delete extracted directory. NOTE: This will delete the local project directory. Do we ever need to do this?
        # try:
        #     time.sleep(4)
        #     FileExtractor.deleteFolder(local_project_dir+"/")
        #     print("RemoteConnectionHandler._execute(): Deleted folder %s"%local_project_dir+"/")
        # except Exception as e:
        #     print(f"RemoteConnectionHandler._execute(): Couldn't delete directory {local_project_dir}. Error:\n{e}")
        # debugging
        # execStopTimeMs=round(time.time()*1000.0)
        # print("RemoteConnectionHandler._execute(): duration %d ms"%(execStopTimeMs-execStartTimeMs))


    @staticmethod
    def path_for_local_project_dir(project_dir):
        """Returns a unique local project dir path, where the project's ZIP-file will be extracted to.

        Args:
            project_dir: Project directory in the execute request message. Absolute path to CLIENT project directory.

        Returns:
            str: Absolute path to a local (server) directory.
        """
        # Convert to path to OS specific PurePath and get the rightmost folder name
        if project_dir.startswith("/"):
            # It's a Posix absolute path
            p = pathlib.PurePosixPath(project_dir).stem
        else:
            # It's a Windows absolute Path
            p = pathlib.PureWindowsPath(project_dir).stem
        return os.path.join(RemoteConnectionHandler.internalProjectFolder, p + "__" + uuid.uuid4().hex)

    @staticmethod
    def convert_input(input_data, local_project_dir):
        """Converts received input data for execution in a local folder.

        Args:
            input_data (dict): Input data as a dict.
            local_project_dir (str): Local (on server) project directory.

        Returns:
            dict: Converted input data
        """
        # Adjust project_dir to point to the local folder
        remote_folder = input_data["project_dir"]  # Project directory on client
        input_data["project_dir"] = local_project_dir  # Project directory on server
        # loop specs
        specsKeys = input_data["specifications"].keys()
        for specKey in specsKeys:
            spec_item = input_data["specifications"][specKey]
            i = 0
            for specItemInfo in spec_item:
                # Adjust definition_file_path in specs to point to the server folder
                if "definition_file_path" in specItemInfo:
                    original_def_file_path = specItemInfo["definition_file_path"]  # Absolute path on client machine
                    # Remove part of definition file path that references client machine path to get
                    # a relative definition file path
                    rel_def_file_path = os.path.relpath(original_def_file_path, remote_folder)
                    modified = os.path.join(local_project_dir, rel_def_file_path)  # Absolute path on server machine
                    input_data["specifications"][specKey][i]["definition_file_path"] = modified
                # Force execute_in_work to False
                if "execute_in_work" in specItemInfo:
                    # print("RemoteConnectionHandler.convert_input(): spec item info contains execute_in_work")
                    input_data["specifications"][specKey][i]["execute_in_work"] = False
                i += 1
        # loop items
        itemsKeys = input_data["items"].keys()
        for itemKey in itemsKeys:
            # force execute_in_work to False in items
            if "execute_in_work" in input_data["items"][itemKey]:
                # print("RemoteConnectionHandler.convert_input() execute_in_work in an item")
                input_data["items"][itemKey]["execute_in_work"] = False
        return input_data

    def _convertTextDictToDicts(self, data):
        newData = dict()
        # print("_convertTextDictToDicts() items: %s"%type(data["items"]))
        newData["items"] = ast.literal_eval(data["items"])
        newData["connections"] = ast.literal_eval(data["connections"])
        newData["specifications"] = ast.literal_eval(data["specifications"])
        newData["node_successors"] = ast.literal_eval(data["node_successors"])
        newData["execution_permits"] = ast.literal_eval(data["execution_permits"])
        newData["settings"] = ast.literal_eval(data["settings"])
        newData["settings"] = ast.literal_eval(data["settings"])
        newData["project_dir"] = data["project_dir"]
        return newData
