######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Engine contributors
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
"""

import json
import os
import threading
import uuid
import zmq  # pylint: disable=unused-import
from spine_engine.server.service_base import ServiceBase
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.util.zip_handler import ZipHandler
from spine_engine.utils.helpers import get_file_size


class ProjectExtractorService(threading.Thread, ServiceBase):
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
        super(ProjectExtractorService, self).__init__(name="ProjectExtractorServiceThread")
        ServiceBase.__init__(self, context, request, job_id)

    def run(self):
        """Extracts the project into a new directory and sends a response back to client."""
        self.worker_socket.connect("inproc://backend")
        dir_name = self.request.data()
        file_names = self.request.filenames()
        if not len(file_names) == 1:  # No file name included
            print("Received msg contained no file name for the ZIP file")
            self.send_completed_with_error("Project ZIP file name missing")
            return
        if not dir_name:
            print("Project name missing from request. Cannot create a local project directory.")
            self.send_completed_with_error("Project name missing")
            return
        if not self.request.zip_file():
            print("Project ZIP file missing from request")
            self.send_completed_with_error("Project ZIP file missing")
            return
        # Make a new local project directory based on project name in request
        local_project_dir = os.path.join(
            ProjectExtractorService.INTERNAL_PROJECT_DIR, dir_name + "__" + uuid.uuid4().hex
        )
        # Create project directory
        try:
            os.makedirs(local_project_dir)
        except OSError:
            print(f"Creating project directory '{local_project_dir}' failed")
            msg = f"Server failed in creating a project directory for the received project '{local_project_dir}'"
            self.send_completed_with_error(msg)
            return
        # Save the received ZIP file
        zip_path = os.path.join(local_project_dir, file_names[0])
        try:
            with open(zip_path, "wb") as f:
                f.write(self.request.zip_file())
        except Exception as e:
            print(f"Saving the received file to '{zip_path}' failed. [{type(e).__name__}: {e}")
            msg = f" [{type(e).__name__}] Server failed in saving the received file to '{zip_path}'"
            self.send_completed_with_error(msg)
            return
        # Check that the size of received bytes and the saved ZIP file match
        if not len(self.request.zip_file()) == os.path.getsize(zip_path):
            print(
                f"Error: Size mismatch in saving ZIP file. Received bytes:{len(self.request.zip_file())}. "
                f"ZIP file size:{os.path.getsize(zip_path)}"
            )
        # Extract the saved file
        print(f"Extracting {file_names[0]} [{get_file_size(os.path.getsize(zip_path))}] to: {local_project_dir}")
        try:
            ZipHandler.extract(zip_path, local_project_dir)
        except Exception as e:
            print(f"File extraction failed: {type(e).__name__}: {e}")
            msg = f"{type(e).__name__}: {e}. - File extraction failed on Server"
            self.send_completed_with_error(msg)
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

    def send_completed_with_error(self, msg):
        """Sends completed message to frontend for relaying to client when something goes wrong."""
        error_event = "remote_execution_init_failed", msg
        self.request.send_response(self.worker_socket, error_event, (self.job_id, "completed"))
