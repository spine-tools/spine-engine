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
import ast
import pathlib
import uuid
import zmq
from spine_engine import SpineEngine
from spine_engine.server.util.file_extractor import FileExtractor
from spine_engine.server.util.event_data_converter import EventDataConverter


class RemoteExecutionHandler:
    """Handles one remote connection at a time from Spine Toolbox,
    executes a DAG, and returns response to the client."""

    # location, where all projects will be extracted and executed
    internalProjectFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "received_projects")

    def __init__(self, context, connection):
        """
        Args:
            context (zmq.Context): Context for this handler.
            connection (ZMQConnection): Socket and message bundled together
        """
        self.context = context
        self.connection = connection

    # def _execute(self):
    #     """Executes a query with the Spine engine, and returns a response to the Zero-MQ client."""
    #     # execStartTimeMs = round(time.time() * 1000.0)
    #     # Parse JSON message
    #     worker_socket = self.context.socket(zmq.DEALER)
    #     worker_socket.connect("inproc://backend")
    #     worker_ctrl_socket = self.context.socket(zmq.PAIR)
    #     worker_ctrl_socket.connect("inproc://worker_ctrl")
    #     poller = zmq.Poller()
    #     print(f"_execute(): thread: {threading.current_thread()}")
    #     poller.register(worker_socket, zmq.POLLIN)
    #     poller.register(worker_ctrl_socket, zmq.POLLIN)
    #     while True:
    #         socks = dict(poller.poll())
    #         if socks.get(worker_socket) == zmq.POLLIN:
    #             print("backend receiving message")
    #             ident, msg, zip_file = worker_socket.recv_multipart()
    #             # Make some rudimentary checks before sending the message for processing
    #             if not self.check_msg_integrity(message, frontend):
    #                 continue
    #             # Start a worker for processing the message
    #             server_msg = ServerMessageParser.parse(message.decode("utf-8"))
    #             cmd = server_msg.getCommand()
    #             if cmd == "ping":  # Handle pings
    #                 print("Handling ping request")
    #                 RemotePingHandler.handlePing(parsed_msg, conn)
    #             elif cmd == "execute":  # Handle execute messages
    #                 # if not len(message[1:]) == 2:  # Skip first part of the message (identity added by DEALER)
    #                 #     print(f"Not enough parts in received msg. Should be 2.")
    #                 #     self.send_error_reply(frontend, "", "",
    #                 #                           f"Message should have two parts. len(message): {len(message)}")
    #                 #     continue
    #                 print("Handling execute request")
    #                 backend.send_multipart([ident, message, zip_file])
    #             else:  # Unknown command
    #                 print(f"Unknown command '{cmd}' received. Sending 'Unknown command' response'")
    #                 self.send_error_reply(frontend, "", "", "Unknown command '{cmd}'")
    #                 continue
    #
    #             print(ident)
    #             # Do execution
    #             self.do_execution(ident, msg, zip_file, worker_socket)
    #             break
    #         if socks.get(worker_ctrl_socket) == zmq.POLLIN:
    #             print("Killing worker thread")
    #             # Kill worker thread
    #             break
    #     print("Closing worker sockets")
    #     worker_socket.close()
    #     worker_ctrl_socket.close()

    def execute(self):
        connection_id = self.connection.connection_id()
        req_id = self.connection.request_id()
        msg_data = self.connection.data()
        file_names = self.connection.filenames()
        if not len(file_names) == 1:  # No file name included
            print("Received msg contained no file name for the zip-file")
            self.connection.send_error_reply("Zip-file name missing")
            return
        if not msg_data["project_dir"]:
            print("Key project_dir missing from received msg. Can not create a local project directory.")
            self.connection.send_error_reply("Problem in execute request. Key 'project_dir' was "
                                             "None or an empty string.")
            return
        if not self.connection.zip_file():
            print("Project zip file missing from request")
            self.connection.send_error_reply("Project zip-file missing from request")
            return
        # Solve a new local directory name based on project_dir
        local_project_dir = self.path_for_local_project_dir(msg_data["project_dir"])
        # Create project directory
        try:
            os.makedirs(local_project_dir)
        except OSError:
            print(f"Creating project directory '{local_project_dir}' failed")
            self.connection.send_error_reply(f"Server failed in creating a project "
                                             f"directory for the received project '{local_project_dir}'")
            return
        # Save the received zip file
        zip_path = os.path.join(local_project_dir, file_names[0])
        try:
            with open(zip_path, "wb") as f:
                f.write(self.connection.zip_file())
        except Exception as e:
            print(f"Saving the received file to '{zip_path}' failed. [{type(e).__name__}: {e}")
            self.connection.send_error_reply(f"Server failed in saving the received file to "
                                             f"'{zip_path}' ({type(e).__name__} at server)")
            return
        # Check that the size of received bytes and the saved zip-file match
        if not len(self.connection.zip_file()) == os.path.getsize(zip_path):
            print(f"Error: Size mismatch in saving zip-file. Received bytes:{len(self.connection.zip_file())}. "
                  f"Zip-file size:{os.path.getsize(zip_path)}")
        # Extract the saved file
        print(f"Extracting project file {file_names[0]} [{os.path.getsize(zip_path)}B] to: {local_project_dir}")
        try:
            FileExtractor.extract(zip_path, local_project_dir)
        except Exception as e:
            print(f"File extraction failed: {type(e).__name__}: {e}")
            self.connection.send_error_reply(f"{type(e).__name__}: {e}. - File extraction failed on Server")
            return
        # Execute DAG in the Spine engine
        print("Executing DAG...")
        converted_data = self.convert_input(msg_data, local_project_dir)
        print(f"converted_engine_data")
        print(f"[items]: {converted_data['items']}")
        print(f"[specifications]: {converted_data['specifications']}")
        print(f"[connections]: {converted_data['connections']}")
        print(f"[execution_permits]: {converted_data['execution_permits']}")
        print(f"[project_dir]: {converted_data['project_dir']}")
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
            self.connection.send_error_reply(f"{type(e).__name__}: {e}. - Project execution failed on Server")
            return
        # Create a response message, send it
        print("Execution done")
        json_events_data = EventDataConverter.convert(event_data)
        self.connection.send_response(json_events_data)
        print(f"Response to request {req_id} sent to client {connection_id}")
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
                    # Remove part of definition file path that references client machine path to get
                    # a relative definition file path
                    rel_def_file_path = os.path.relpath(original_def_file_path, remote_folder)
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
