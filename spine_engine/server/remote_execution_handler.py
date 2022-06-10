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
Contains RemoteExecutionHandler class for receiving control messages(with project content) from the Toolbox,
and running a DAG with Spine Engine. Only one DAG can be executed at a time.
:authors: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   24.08.2021
"""

import os
import pathlib
import threading
import uuid
import zmq
from spine_engine import SpineEngine
from spine_engine.server.util.file_extractor import FileExtractor
from spine_engine.server.util.event_data_converter import EventDataConverter


class RemoteExecutionHandler(threading.Thread):
    """Handles one execute request at a time from Spine Toolbox,
    executes a DAG, and returns response to the client."""

    # location, where all projects will be extracted and executed
    internalProjectFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "received_projects")

    def __init__(self, context, request):
        """
        Args:
            context (zmq.Context): Context for this handler.
            request (Request): Request from client
        """
        super().__init__(name="ExecutionHandlerThread")
        self.context = context
        self.worker_socket = self.context.socket(zmq.DEALER)
        self.request = request

    def run(self):
        """Handles an execute DAG request."""
        self.worker_socket.connect("inproc://backend")
        msg_data = self.request.data()
        file_names = self.request.filenames()
        if not len(file_names) == 1:  # No file name included
            print("Received msg contained no file name for the zip-file")
            self.request.send_error_reply(self.worker_socket, "Zip-file name missing")
            return
        if not msg_data["project_dir"]:
            print("Key project_dir missing from received msg. Can not create a local project directory.")
            self.request.send_error_reply(self.worker_socket, "Problem in execute request. Key 'project_dir' was "
                                             "None or an empty string.")
            return
        if not self.request.zip_file():
            print("Project zip-file missing from request")
            self.request.send_error_reply(self.worker_socket, "Project zip-file missing from request")
            return
        # Solve a new local directory name based on project_dir
        local_project_dir = self.path_for_local_project_dir(msg_data["project_dir"])
        # Create project directory
        try:
            os.makedirs(local_project_dir)
        except OSError:
            print(f"Creating project directory '{local_project_dir}' failed")
            self.request.send_error_reply(self.worker_socket, f"Server failed in creating a project "
                                             f"directory for the received project '{local_project_dir}'")
            return
        # Save the received zip file
        zip_path = os.path.join(local_project_dir, file_names[0])
        try:
            with open(zip_path, "wb") as f:
                f.write(self.request.zip_file())
        except Exception as e:
            print(f"Saving the received file to '{zip_path}' failed. [{type(e).__name__}: {e}")
            self.request.send_error_reply(self.worker_socket, f"Server failed in saving the received file to "
                                             f"'{zip_path}' ({type(e).__name__} at server)")
            return
        # Check that the size of received bytes and the saved zip-file match
        if not len(self.request.zip_file()) == os.path.getsize(zip_path):
            print(f"Error: Size mismatch in saving zip-file. Received bytes:{len(self.request.zip_file())}. "
                  f"Zip-file size:{os.path.getsize(zip_path)}")
        # Extract the saved file
        print(f"Extracting project file {file_names[0]} [{os.path.getsize(zip_path)}B] to: {local_project_dir}")
        try:
            FileExtractor.extract(zip_path, local_project_dir)
        except Exception as e:
            print(f"File extraction failed: {type(e).__name__}: {e}")
            self.request.send_error_reply(self.worker_socket, f"{type(e).__name__}: {e}. - "
                                                                 f"File extraction failed on Server")
            return
        # Execute DAG in the Spine engine
        print("Executing DAG...")
        converted_data = self.convert_input(msg_data, local_project_dir)
        try:
            engine = SpineEngine(**converted_data)
            # get events+data from the spine engine
            event_data = []
            while True:
                # NOTE! All execution event messages generated while running the DAG are collected into a single list.
                # This list is sent back to Toolbox in a single message only after the whole DAG has finished execution
                # in a single message. This means that Spine Toolbox cannot update the GUI (e.g. animations in Design
                # View don't work, and the execution progress is not updated to Item Execution Log (or other Logs)
                # until all items have finished.
                event_type, data = engine.get_event()
                event_data.append((event_type, data))
                if data == "COMPLETED" or data == "FAILED":
                    break
        except Exception as e:
            print(f"Execution failed: {type(e).__name__}: {e}")
            self.request.send_error_reply(self.worker_socket, f"{type(e).__name__}: {e}. - "
                                                                 f"Project execution failed on Server")
            return
        # Create a response message, send it back to frontend
        print("Execution done")
        json_events_data = EventDataConverter.convert(event_data)
        self.request.send_response(self.worker_socket, json_events_data)
        # delete extracted directory. NOTE: This will delete the local project directory. Do we ever need to do this?
        # try:
        #     time.sleep(4)
        #     FileExtractor.deleteFolder(local_project_dir+"/")
        #     print("RemoteExecutionHandler._execute(): Deleted folder %s"%local_project_dir+"/")
        # except Exception as e:
        #     print(f"RemoteExecutionHandler._execute(): Couldn't delete directory {local_project_dir}. Error:\n{e}")
        # debugging
        # execStopTimeMs=round(time.time()*1000.0)
        # print("RemoteExecutionHandler._execute(): duration %d ms"%(execStopTimeMs-execStartTimeMs))

    def close(self):
        """Cleans up after execution."""
        print(f"Closing worker {self.request.connection_id()}")
        self.worker_socket.close()

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
        return os.path.join(RemoteExecutionHandler.internalProjectFolder, p + "__" + uuid.uuid4().hex)

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
        # Loop specs
        specs_keys = input_data["specifications"].keys()
        for specs_key in specs_keys:
            spec_item = input_data["specifications"][specs_key]
            i = 0
            for specItemInfo in spec_item:
                # Adjust definition_file_path in specs to point to the server folder
                if "definition_file_path" in specItemInfo:
                    original_def_file_path = specItemInfo["definition_file_path"]  # Absolute path on client machine
                    # Make sure path separators match the OS separator
                    original_def_file_path = original_def_file_path.replace("\\", os.path.sep)
                    # Remove part of definition file path that references client machine path to get
                    # a relative definition file path. Note: os.path.relpath() does not work because the output
                    # depends on OS. Note2: '/' must be added to remote folder here.
                    rel_def_file_path = original_def_file_path.replace(remote_folder + "/", "")
                    modified = os.path.join(local_project_dir, rel_def_file_path)  # Absolute path on server machine
                    print(f"\noriginal def_file_path: {original_def_file_path}")
                    print(f"remote_folder: {remote_folder}")
                    print(f"relative def_file_path: {rel_def_file_path}")
                    print(f"updated def_file_path: {modified}\n")
                    input_data["specifications"][specs_key][i]["definition_file_path"] = modified
                # Force execute_in_work to False
                if "execute_in_work" in specItemInfo:
                    # print("RemoteExecutionHandler.convert_input(): spec item info contains execute_in_work")
                    input_data["specifications"][specs_key][i]["execute_in_work"] = False
                i += 1
        # Loop items
        items_keys = input_data["items"].keys()
        for items_key in items_keys:
            # force execute_in_work to False in items
            if "execute_in_work" in input_data["items"][items_key]:
                # print("RemoteExecutionHandler.convert_input() execute_in_work in an item")
                input_data["items"][items_key]["execute_in_work"] = False
        return input_data
