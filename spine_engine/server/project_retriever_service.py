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
Contains a class for sending a finished project back to client.
"""

import json
import os
import shutil
import threading
import zmq  # pylint: disable=unused-import
from spine_engine.server.service_base import ServiceBase
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.utils.helpers import get_file_size


class ProjectRetrieverService(threading.Thread, ServiceBase):
    """Class for transmitting a project back to client."""

    def __init__(self, context, request, job_id, project_dir):
        """Initializes instance.

        Args:
            context (zmq.Context): Server context
            request (Request): Client request
            job_id (str): Thread job Id
            project_dir (str): Absolute path to project directory
        """
        super(ProjectRetrieverService, self).__init__(name="ProjectRetrieverServiceThread")
        ServiceBase.__init__(self, context, request, job_id)
        self.project_dir = project_dir

    def run(self):
        """Replies to a retrieve_project command."""
        self.worker_socket.connect("inproc://backend")
        zip_fname = "project_package"  # make_archive() adds extension (.zip)
        dest_dir = os.path.join(self.project_dir, os.pardir)  # Parent dir of project_dir
        zip_path = os.path.join(dest_dir, zip_fname)
        try:
            shutil.make_archive(zip_path, "zip", self.project_dir)
        except OSError:
            msg = f"Zipping project {self.project_dir} to {zip_path} failed"
            print(msg)
            self.send_completed_with_error(msg)
            return
        zip_fpath = os.path.abspath(os.path.join(self.project_dir, os.pardir, zip_fname + ".zip"))
        if not os.path.isfile(zip_fpath):
            msg = f"Zip file {zip_fpath} does not exist"
            print(msg)
            self.send_completed_with_error(msg)
            return
        file_size = get_file_size(os.path.getsize(zip_fpath))
        print(f"Transmitting file [{file_size}]: {zip_fpath} to client")
        # Read file into bytes string and transmit
        with open(zip_fpath, "rb") as f:
            file_data = f.read()
        reply_msg = ServerMessage(self.request.cmd(), self.request.request_id(), "", ["project_package.zip"])
        internal_msg = json.dumps((self.job_id, "completed"))
        self.request.send_multipart_reply_with_file(
            self.worker_socket, self.request.connection_id(), reply_msg.to_bytes(), file_data, internal_msg
        )

    def send_completed_with_error(self, msg):
        """Sends completed message to frontend for relaying to client when something goes wrong."""
        error_event = "project_retriever_service_failed", msg
        self.request.send_response(self.worker_socket, error_event, (self.job_id, "completed"))
