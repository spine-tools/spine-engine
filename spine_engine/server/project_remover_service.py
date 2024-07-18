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

import os
import shutil
import threading
import zmq  # pylint: disable=unused-import
from spine_engine.server.service_base import ServiceBase


class ProjectRemoverService(threading.Thread, ServiceBase):
    """Class for removing a project folder from server."""

    def __init__(self, context, request, job_id, project_dir):
        """Initializes instance.

        Args:
            context (zmq.Context): Server context
            request (Request): Client request
            job_id (str): Thread job Id
            project_dir (str): Absolute path to project directory
        """
        super(ProjectRemoverService, self).__init__(name="ProjectRemoverServiceThread")
        ServiceBase.__init__(self, context, request, job_id)
        self.project_dir = project_dir

    def run(self):
        """Removes a used project directory from server."""
        self.worker_socket.connect("inproc://backend")
        int_msg = (self.job_id, "completed")
        if not os.path.isdir(self.project_dir):
            print(f"Project dir {self.project_dir} has been removed already.")
            self.request.send_response(self.worker_socket, ("server_event", "ok"), int_msg)
            return
        try:
            shutil.rmtree(self.project_dir)
        except OSError as e:
            print(f"[OSError]: {e}\nRemoving project dir failed. Moving on...")
            self.request.send_response(self.worker_socket, ("server_event", "fine"), int_msg)
            return
        self.request.send_response(self.worker_socket, ("server_event", "completed"), int_msg)
