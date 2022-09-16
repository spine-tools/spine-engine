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
Contains a class for extracting a project ZIP file into a new directory on server.
:authors: P. Savolainen (VTT)
:date:   29.8.2022
"""

import os
import json
import threading
import uuid
import zmq
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.util.zip_handler import ZipHandler


class ProjectExtractorService(threading.Thread):
    """Class for handling 'prepare_execution' requests."""

    # Root directory, where all projects will be extracted and executed
    INTERNAL_PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "received_projects")

    def __init__(self, context, request, job_id):
        """Initializes instance.

        Args:
            context (zmq.Context): Server context
            request (Request): Client request
            job_id (str): Worker thread Id
        """
        super().__init__(name="ProjectExtractorServiceThread")
        self.context = context
        self.request = request
        self.job_id = job_id
        self.worker_socket = self.context.socket(zmq.DEALER)  # Backend socket

    def run(self):
        """Extracts the project into a new directory and sends a response back to client."""
        self.worker_socket.connect("inproc://backend")
        dir_name = self.request.data()
        file_names = self.request.filenames()
        if not len(file_names) == 1:  # No file name included
            print("Received msg contained no file name for the ZIP file")
            self.request.send_response(
                self.worker_socket, ("remote_execution_init_failed", "Project ZIP file name missing"), (self.job_id, ""))
            return
        if not dir_name:
            print("Project name missing from request. Cannot create a local project directory.")
            self.request.send_response(
                self.worker_socket, ("remote_execution_init_failed", "Project name missing"),
                (self.job_id, "")
            )
            return
        if not self.request.zip_file():
            print("Project ZIP file missing from request")
            self.request.send_response(
                self.worker_socket, ("remote_execution_init_failed", "Project ZIP file missing"), (self.job_id, ""))
            return
        # Make a new local project directory based on project name in request
        local_project_dir = os.path.join(ProjectExtractorService.INTERNAL_PROJECT_DIR, dir_name + "__" + uuid.uuid4().hex)
        # Create project directory
        try:
            os.makedirs(local_project_dir)
        except OSError:
            print(f"Creating project directory '{local_project_dir}' failed")
            self.request.send_response(
                self.worker_socket,
                ("remote_execution_init_failed", f"Server failed in creating a project directory "
                                                 f"for the received project '{local_project_dir}'"),
                (self.job_id, "")
            )
            return
        # Save the received ZIP file
        zip_path = os.path.join(local_project_dir, file_names[0])
        try:
            with open(zip_path, "wb") as f:
                f.write(self.request.zip_file())
        except Exception as e:
            print(f"Saving the received file to '{zip_path}' failed. [{type(e).__name__}: {e}")
            self.request.send_response(
                self.worker_socket,
                ("remote_execution_init_failed", f"Server failed in saving the received file "
                                                 f"to '{zip_path}' ({type(e).__name__} at server)"),
                (self.job_id, "")
            )
            return
        # Check that the size of received bytes and the saved ZIP file match
        if not len(self.request.zip_file()) == os.path.getsize(zip_path):
            print(f"Error: Size mismatch in saving ZIP file. Received bytes:{len(self.request.zip_file())}. "
                  f"ZIP file size:{os.path.getsize(zip_path)}")
        # Extract the saved file
        print(f"Extracting project file {file_names[0]} [{os.path.getsize(zip_path)}B] to: {local_project_dir}")
        try:
            ZipHandler.extract(zip_path, local_project_dir)
        except Exception as e:
            print(f"File extraction failed: {type(e).__name__}: {e}")
            self.request.send_response(
                self.worker_socket,
                ("remote_execution_init_failed", f"{type(e).__name__}: {e}. - File extraction failed on Server"),
                (self.job_id, "")
            )
            return
        # Remove extracted ZIP file
        try:
            os.remove(zip_path)
        except OSError:
            print(f"[OSError] File: {zip_path} was not removed")
        reply_msg = ServerMessage(self.request.cmd(), self.job_id, "", None)
        internal_msg = json.dumps((self.job_id, local_project_dir))
        self.request.send_multipart_reply(
            self.worker_socket, self.request.connection_id(), reply_msg.to_bytes(), internal_msg
        )

    def close(self):
        """Closes socket and cleans up."""
        self.worker_socket.close()
