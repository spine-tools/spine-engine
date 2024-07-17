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
Unit tests for ProjectExtractorService class.
"""

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest import mock
import zmq
from spine_engine.server.engine_server import EngineServer, ServerSecurityModel
from spine_engine.server.util.server_message import ServerMessage


class TestProjectExtractorService(unittest.TestCase):
    def setUp(self):
        self._temp_dir = TemporaryDirectory()
        self.service = EngineServer("tcp", 5559, ServerSecurityModel.NONE, "")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect("tcp://localhost:5559")

    def tearDown(self):
        self._temp_dir.cleanup()
        self.service.close()
        if not self.socket.closed:
            self.socket.close()
        if not self.context.closed:
            self.context.term()

    @mock.patch(
        "spine_engine.server.project_extractor_service.ProjectExtractorService.INTERNAL_PROJECT_DIR",
        new_callable=mock.PropertyMock,
    )
    def test_project_extraction(self, mock_proj_dir):
        mock_proj_dir.return_value = self._temp_dir.name
        with open(os.path.join(str(Path(__file__).parent), "zippedproject.zip"), "rb") as f:
            file_data = f.read()
        msg = ServerMessage("prepare_execution", "1", json.dumps("project_name"), ["zippedproject.zip"])
        self.socket.send_multipart([msg.to_bytes(), file_data])
        response = self.socket.recv_multipart()
        response_msg = ServerMessage.parse(response[1])
        self.assertEqual("prepare_execution", response_msg.getCommand())
        self.assertTrue(len(response_msg.getId()) == 32)
        self.assertEqual("", response_msg.getData())

    def test_project_file_name_missing(self):
        """File name list in request is empty."""
        with open(os.path.join(str(Path(__file__).parent), "zippedproject.zip"), "rb") as f:
            file_data = f.read()
        msg = ServerMessage("prepare_execution", "1", json.dumps("project_name"), [])
        self.socket.send_multipart([msg.to_bytes(), file_data])
        response = self.socket.recv_multipart()
        self.check_correct_error_response(response[1], "Project ZIP file name missing")

    def test_project_name_missing(self):
        """Project name missing from request."""
        with open(os.path.join(str(Path(__file__).parent), "zippedproject.zip"), "rb") as f:
            data_file = f.read()
        msg = ServerMessage("prepare_execution", "1", "", ["zippedproject.zip"])
        self.socket.send_multipart([msg.to_bytes(), data_file])
        response = self.socket.recv_multipart()
        self.check_correct_error_response(response[1], "Project name missing")

    def test_zip_file_missing(self):
        """Project zip-file missing from request."""
        msg = ServerMessage("prepare_execution", "1", json.dumps("project_name"), ["missing_zip_file.zip"])
        self.socket.send_multipart([msg.to_bytes()])
        response = self.socket.recv_multipart()
        self.check_correct_error_response(response[1], "Project ZIP file missing")

    def check_correct_error_response(self, response, expected_err_str):
        response_msg = ServerMessage.parse(response)
        self.assertEqual("prepare_execution", response_msg.getCommand())
        self.assertEqual("1", response_msg.getId())
        data = response_msg.getData()
        self.assertEqual("remote_execution_init_failed", data[0])
        self.assertEqual(expected_err_str, data[1])


if __name__ == "__main__":
    unittest.main()
