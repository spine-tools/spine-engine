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
Contains a class for remote persistent execution manager related actions.
:authors: P. Savolainen (VTT)
:date:   26.9.2022
"""

import threading
import json
import zmq
from spine_engine.server.service_base import ServiceBase


class PersistentExecutionService(threading.Thread, ServiceBase):
    """Class for interacting with a persistent execution manager running on server."""
    def __init__(self, context, request, job_id, persistent_exec_mngr):
        """
        Args:
            context (zmq.Context): Context for this handler.
            request (Request): Client request
            job_id (str): Worker thread Id
            persistent_exec_mngr (PersistentExecutionManagerBase): Persistent execution manager
        """
        super(PersistentExecutionService, self).__init__(name="PersistentExecutionService")
        ServiceBase.__init__(self, context, request, job_id)
        self.persistent_exec_mngr = persistent_exec_mngr
        self.push_socket = self.context.socket(zmq.PUSH)

    def run(self):
        """Executes client's command in execution service and returns the response back to client."""
        self.worker_socket.connect("inproc://backend")
        pub_port = self.push_socket.bind_to_random_port("tcp://*")
        pm = self.persistent_exec_mngr._persistent_manager
        cmd_type = self.request.data()[1]  # Command type for persistent manager, e.g. 'is_complete'
        cmd = self.request.data()[2]  # Command to process in persistent manager
        if cmd_type == "is_complete":
            retval = pm.make_complete_command(cmd)
            self.request.send_response(self.worker_socket, (cmd_type, retval), (self.job_id, ""))
        elif cmd_type == "issue_persistent_command":
            self.request.send_response(self.worker_socket, (cmd_type, str(pub_port)), (self.job_id, "in_progress"))
            for msg in pm.issue_command(cmd, add_history=True, catch_exception=False):
                json_msg = json.dumps(msg)
                self.push_socket.send(json_msg.encode("utf-8"))  # This blocks until somebody is pulling (receiving)
            self.push_socket.send(b'END')
            self.request.send_response(self.worker_socket, (cmd_type, "everything ok"), (self.job_id, "completed"))
        elif cmd_type == "get_completions":
            retval = pm.get_completions(cmd)
            self.request.send_response(self.worker_socket, (cmd_type, retval), (self.job_id, ""))
        elif cmd_type == "get_history_item":
            text, prefix, backwards = cmd
            retval = pm.get_history_item(text, prefix, backwards)
            self.request.send_response(self.worker_socket, (cmd_type, retval), (self.job_id, ""))
        elif cmd_type == "restart_persistent":
            self.request.send_response(self.worker_socket, (cmd_type, str(pub_port)), (self.job_id, "in_progress"))
            for msg in pm.restart_persistent():
                json_msg = json.dumps(msg)
                self.push_socket.send(json_msg.encode("utf-8"))
            self.push_socket.send(b'END')
            self.request.send_response(self.worker_socket, (cmd_type, "everything ok"), (self.job_id, "completed"))
        elif cmd_type == "interrupt_persistent":
            retval = pm.interrupt_persistent()
            self.request.send_response(self.worker_socket, (cmd_type, retval), (self.job_id, ""))
        else:
            print(f"Command type {cmd_type} does not have a handler. cmd:{cmd}")
            self.request.send_response(self.worker_socket, (cmd_type, "Unhandled command"), (self.job_id, ""))

    def close(self):
        """Cleans up after thread closes."""
        super().close()
        self.push_socket.close()
