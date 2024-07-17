#####################################################################################################################
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
Unit tests for ProjectRemoverService class.
"""

import json
import os
from pathlib import Path
import unittest
import zmq
from spine_engine.server.engine_server import EngineServer, ServerSecurityModel
from spine_engine.server.util.server_message import ServerMessage


class TestProjectRemoverService(unittest.TestCase):
    def setUp(self):
        self.service = EngineServer("tcp", 5559, ServerSecurityModel.NONE, "")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect("tcp://localhost:5559")

    def tearDown(self):
        self.service.close()
        if not self.socket.closed:
            self.socket.close()
        if not self.context.closed:
            self.context.term()

    def test_remove_project_service(self):
        """Extracts a project into received_projects folder, then removes it using the ProjectRemoverService."""
        with open(os.path.join(str(Path(__file__).parent), "zippedproject.zip"), "rb") as f:
            file_data = f.read()
        msg = ServerMessage("prepare_execution", "1", json.dumps("project_name"), ["zippedproject.zip"])
        self.socket.send_multipart([msg.to_bytes(), file_data])
        r1 = self.socket.recv_multipart()
        r1_msg = ServerMessage.parse(r1[1])
        self.assertEqual("prepare_execution", r1_msg.getCommand())
        self.assertTrue(len(r1_msg.getId()) == 32)
        self.assertEqual("", r1_msg.getData())
        job_id = r1_msg.getId()
        req = ServerMessage("remove_project", job_id, "")
        self.socket.send_multipart([req.to_bytes()])
        r2 = self.socket.recv_multipart()
        r2_msg = ServerMessage.parse(r2[1])
        self.assertEqual("remove_project", r2_msg.getCommand())
        self.assertEqual(["server_event", "completed"], r2_msg.getData())

    def test_project_dir_not_found(self):
        """Sends an invalid job_id to server and check that response is as expected."""
        msg = ServerMessage("remove_project", "12345", "")  # Invalid job_id on purpose
        self.socket.send_multipart([msg.to_bytes()])
        r = self.socket.recv_multipart()
        r_msg = ServerMessage.parse(r[1])
        self.assertEqual("server_init_failed", r_msg.getData()[0])


if __name__ == "__main__":
    unittest.main()
