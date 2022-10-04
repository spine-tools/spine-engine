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
Contains RemoteExecutionService class that executes a single DAG on the Spine Engine Server.
:authors: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   24.08.2021
"""

import os
import threading
import zmq
from spine_engine import SpineEngine
from spine_engine.server.service_base import ServiceBase
from spine_engine.server.util.event_data_converter import EventDataConverter
from spine_engine.server.util.zip_handler import ZipHandler


class RemoteExecutionService(threading.Thread, ServiceBase):
    """Executes a DAG contained in the client request. Project must
    be on server before running this service."""
    def __init__(self, context, request, job_id, project_dir, persistent_exec_mngr_q):
        """
        Args:
            context (zmq.Context): Context for this handler.
            request (Request): Client request
            job_id (str): Worker thread Id
            project_dir (str): Absolute path to a server directory where the project has been extracted to
            persistent_exec_mngr_q (queue.Queue): Queue for storing persistent exec. managers (consumed in frontend)
        """
        super(RemoteExecutionService, self).__init__(name="RemoteExecutionServiceThread")
        ServiceBase.__init__(self, context, request, job_id)
        self.pub_socket = self.context.socket(zmq.PUB)
        self.local_project_dir = project_dir
        self.persistent_keys = dict()  # Mapping of item_name to a persistent execution manager key
        self.persistent_exec_mngrs = dict()  # Mapping of per. execution manager key to per. execution manager
        self.persist_q = persistent_exec_mngr_q

    def collect_persistent_keys(self, event_type, data):
        """Collects the keys used in identifying persistent execution managers
        The key is in a persistent_execution_msg when the type is persistent_started."""
        if event_type == "persistent_execution_msg" and data["type"] == "persistent_started":
            self.persistent_keys[data["item_name"]] = data["key"]  # NOTE: Cast tuple into list

    def collect_persistent_console_managers(self, event_type, data, running_items):
        """Collects a persistent execution manager from a tool item that is being
        executed in engine (running). Matches the key (collected earlier), with the
        persistent execution manager and inserts them into a dict."""
        if event_type == "persistent_execution_msg" and data["type"] == "execution_started":
            persistent_owner = data["item_name"]
            if len(running_items) > 0:
                for item in running_items:
                    if item.name == persistent_owner:
                        ref = item._tool_instance.exec_mngr
                        k = self.persistent_keys[persistent_owner]
                        self.persistent_exec_mngrs[k] = ref
            else:  # If this happens regularly, we have a problem
                print(f"[DEBUG] Collecting {persistent_owner}'s persistent exec. manager failed. Item not running.")

    def run(self):
        """Sends an execution started response to start execution request. Runs Spine Engine
        and sends the events to the client using a publish socket."""
        self.worker_socket.connect("inproc://backend")
        pub_port = self.pub_socket.bind_to_random_port("tcp://*")
        engine_data = self.request.data()
        print("Executing DAG...")
        # Send execution started message to client with the publish socket port
        self.request.send_response(
            self.worker_socket, ("remote_execution_started", str(pub_port)), (self.job_id, "in_progress"))
        converted_data = self.convert_input(engine_data, self.local_project_dir)
        try:
            engine = SpineEngine(**converted_data)
            while True:
                # Get event and associated data from the spine engine
                event_type, data = engine.get_event()
                self.collect_persistent_keys(event_type, data)
                self.collect_persistent_console_managers(event_type, data, engine._running_items)
                json_event = EventDataConverter.convert(event_type, data)
                # Send events using a publish socket
                self.pub_socket.send_multipart([b"EVENTS", json_event.encode("utf-8")])
                if data == "COMPLETED" or data == "FAILED":
                    break
        except Exception as e:
            print(f"Execution failed: {type(e).__name__}: {e}")
            json_error_event = EventDataConverter.convert(
                "server_execution_error", f"{type(e).__name__}: {e}. - Project execution failed on Server"
            )
            self.pub_socket.send_multipart([b"EVENTS", json_error_event.encode("utf-8")])
            return
        self.persist_q.put(self.persistent_exec_mngrs)  # Put new persistent execution managers to queue
        # Note: This is not sent to client
        self.request.send_response(
            self.worker_socket, ("remote_execution_event", "completed"), (self.job_id, "completed"))
        print("Execution done")
        # delete extracted directory. NOTE: This will delete the local project directory. Do we ever need to do this?
        # try:
        #     ZipHandler.delete_folder(self.local_project_dir)
        #     print(f"RemoteExecutionService._execute(): Deleted folder {self.local_project_dir}")
        # except Exception as e:
        #     print(f"RemoteExecutionService._execute(): Couldn't delete directory {self.local_project_dir}. Error:\n{e}")
        # execStopTimeMs=round(time.time()*1000.0)
        # print("RemoteExecutionService._execute(): duration %d ms"%(execStopTimeMs-execStartTimeMs))

    def close(self):
        """Cleans up after thread closes."""
        super().close()
        self.pub_socket.close()

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
                    # print(f"\noriginal def_file_path: {original_def_file_path}")
                    # print(f"remote_folder: {remote_folder}")
                    # print(f"relative def_file_path: {rel_def_file_path}")
                    # print(f"updated def_file_path: {modified}\n")
                    input_data["specifications"][specs_key][i]["definition_file_path"] = modified
                # Force execute_in_work to False
                if "execute_in_work" in specItemInfo:
                    input_data["specifications"][specs_key][i]["execute_in_work"] = False
                i += 1
                if "execution_settings" in specItemInfo:
                    if specItemInfo["execution_settings"]["use_jupyter_console"]:
                        # Replace kernel_spec_name with the default kernel spec 'python3' (must be available on server)
                        specItemInfo["execution_settings"]["kernel_spec_name"] = "python3"
                    else:
                        # Replace Python executable exec with "" because client's Python is not be available on server
                        specItemInfo["execution_settings"]["executable"] = ""
        # Loop items
        items_keys = input_data["items"].keys()
        for items_key in items_keys:
            # force execute_in_work to False in items
            if "execute_in_work" in input_data["items"][items_key]:
                # print("RemoteExecutionService.convert_input() execute_in_work in an item")
                input_data["items"][items_key]["execute_in_work"] = False
        # Edit app settings dictionary
        # Replace Julia path and Julia project path with an empty string so that the server uses the Julia in PATH
        input_data["settings"]["appSettings/juliaPath"] = ""
        input_data["settings"]["appSettings/juliaProjectPath"] = ""
        # Replace Julia kernel
        input_data["settings"]["appSettings/juliaKernel"] = "julia-1.8"  # (must be available on server)
        return input_data
