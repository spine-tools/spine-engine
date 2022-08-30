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
Contains a class for extracting a project zip file.
:authors: P. Savolainen (VTT)
:date:   29.8.2022
"""

import os
import json
import threading
import pathlib
import uuid
import zmq
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.util.file_extractor import FileExtractor


class ProjectExtractor(threading.Thread):
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
        super().__init__(name="ProjectExtractorThread")
        self.context = context
        self.request = request
        self.job_id = job_id
        self.worker_socket = self.context.socket(zmq.DEALER)  # Backend socket

    def run(self):
        """Extracts the project into a new directory and makes and sends a response back to frontend."""
        self.worker_socket.connect("inproc://backend")
        dir_name = self.request.data()
        file_names = self.request.filenames()
        if not len(file_names) == 1:  # No file name included
            print("Received msg contained no file name for the zip-file")
            self.request.send_response(
                self.worker_socket, ("remote_execution_init_failed", "Zip-file name missing"), (self.job_id, ""))
            return
        if not dir_name:
            print("Project dir missing from request. Cannot create a local project directory.")
            self.request.send_response(
                self.worker_socket, ("remote_execution_init_failed", "Problem in prepare_execution request. "
                                                                     "Folder name should be included to request."),
                (self.job_id, "")
            )
            return
        if not self.request.zip_file():
            print("Project zip-file missing from request")
            self.request.send_response(
                self.worker_socket, ("remote_execution_init_failed", "Project zip-file missing"), (self.job_id, ""))
            return
        # Solve a new local directory name based on project_dir
        local_project_dir = os.path.join(ProjectExtractor.INTERNAL_PROJECT_DIR, dir_name + "__" + uuid.uuid4().hex)
        # local_project_dir = self.path_for_local_project_dir(project_dir)
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
        # Save the received zip file
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
            self.request.send_response(
                self.worker_socket,
                ("remote_execution_init_failed", f"{type(e).__name__}: {e}. - File extraction failed on Server"),
                (self.job_id, "")
            )
            return
        reply_msg = ServerMessage(self.request.cmd(), self.job_id, "", None)
        internal_msg = json.dumps((self.job_id, local_project_dir))
        self.request.send_multipart_reply(
            self.worker_socket, self.request.connection_id(), reply_msg.to_bytes(), internal_msg
        )

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
        return os.path.join(ProjectExtractor.INTERNAL_PROJECT_DIR, p + "__" + uuid.uuid4().hex)

    def close(self):
        """Closes socket and cleans up."""
        self.worker_socket.close()
