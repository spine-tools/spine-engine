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
Unit tests for RemoteExecutionService class.
:author: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   24.8.2021
"""

import time
import unittest
import json
import os
import random
from pathlib import Path
from unittest import mock
from tempfile import TemporaryDirectory
import zmq
from spine_engine.server.engine_server import EngineServer, ServerSecurityModel
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.util.event_data_converter import EventDataConverter


class TestRemoteExecutionService(unittest.TestCase):
    def setUp(self):
        self._temp_dir = TemporaryDirectory()
        self.service = EngineServer("tcp", 5559, ServerSecurityModel.NONE, "")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.identity = "Worker1".encode("ascii")
        self.socket.connect("tcp://localhost:5559")
        self.pull_socket = self.context.socket(zmq.PULL)
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.poller.register(self.pull_socket, zmq.POLLIN)

    def tearDown(self):
        self.service.close()
        if not self.socket.closed:
            self.socket.close()
        if not self.pull_socket.closed:
            self.pull_socket.close()
        if not self.context.closed:
            self.context.term()
        try:
            self._temp_dir.cleanup()
        except RecursionError:
            print("RecursionError due to a PermissionError on Windows. Server resources not cleaned up properly.")

    @mock.patch("spine_engine.server.project_extractor_service.ProjectExtractorService.INTERNAL_PROJECT_DIR", new_callable=mock.PropertyMock)
    def test_remote_execution1(self, mock_proj_dir):
        """Tests executing a Hello World (DC -> Python Tool) project."""
        mock_proj_dir.return_value = self._temp_dir.name
        with open(os.path.join(str(Path(__file__).parent), "helloworld.zip"), "rb") as f:
            file_data = f.read()
        prepare_msg = ServerMessage("prepare_execution", "1", json.dumps("Hello World"), ["helloworld.zip"])
        self.socket.send_multipart([prepare_msg.to_bytes(), file_data])
        prepare_response = self.socket.recv_multipart()
        prepare_response_msg = ServerMessage.parse(prepare_response[1])
        job_id = prepare_response_msg.getId()
        self.assertEqual("prepare_execution", prepare_response_msg.getCommand())
        self.assertTrue(len(job_id) == 32)
        self.assertEqual("", prepare_response_msg.getData())
        # Send start_execution request
        engine_data = self.make_engine_data_for_helloworld_project()
        engine_data_json = json.dumps(engine_data)
        start_msg = ServerMessage("start_execution", job_id, engine_data_json, None)
        self.socket.send_multipart([start_msg.to_bytes()])
        start_response = self.socket.recv_multipart()
        start_response_msg = ServerMessage.parse(start_response[1])
        start_response_msg_data = start_response_msg.getData()
        self.assertEqual("remote_execution_started", start_response_msg_data[0])
        self.receive_events(start_response_msg_data[1])

    @mock.patch("spine_engine.server.project_extractor_service.ProjectExtractorService.INTERNAL_PROJECT_DIR", new_callable=mock.PropertyMock)
    def test_remote_execution2(self, mock_project_dir):
        """Tests executing a single DAG project with 3 items (DC -> Importer -> DS)."""
        mock_project_dir.return_value = self._temp_dir.name
        with open(os.path.join(str(Path(__file__).parent), "simple_importer.zip"), "rb") as f:
            file_data = f.read()
        prepare_msg = ServerMessage("prepare_execution", "1", json.dumps("Simple Importer"), ["simple_importer.zip"])
        self.socket.send_multipart([prepare_msg.to_bytes(), file_data])
        prepare_response = self.socket.recv_multipart()
        prepare_response_msg = ServerMessage.parse(prepare_response[1])
        job_id = prepare_response_msg.getId()
        self.assertEqual("prepare_execution", prepare_response_msg.getCommand())
        self.assertTrue(len(job_id) == 32)
        self.assertEqual("", prepare_response_msg.getData())
        # Send start_execution request
        engine_data = self.make_engine_data_for_simple_importer_project()
        engine_data_json = json.dumps(engine_data)
        start_msg = ServerMessage("start_execution", job_id, engine_data_json, None)
        self.socket.send_multipart([start_msg.to_bytes()])
        start_response = self.socket.recv_multipart()
        start_response_msg = ServerMessage.parse(start_response[1])
        start_response_msg_data = start_response_msg.getData()
        self.assertEqual("remote_execution_started", start_response_msg_data[0])
        self.receive_events(start_response_msg_data[1])

    @mock.patch("spine_engine.server.project_extractor_service.ProjectExtractorService.INTERNAL_PROJECT_DIR", new_callable=mock.PropertyMock)
    def test_loop_calls(self, mock_proj_dir):
        """Tests executing the Hellow World project (DC -> Tool) five times in a row."""
        mock_proj_dir.return_value = self._temp_dir.name
        engine_data = self.make_engine_data_for_helloworld_project()
        engine_data_json = json.dumps(engine_data)
        with open(os.path.join(str(Path(__file__).parent), "helloworld.zip"), "rb") as f:
            file_data = f.read()
        for i in range(5):
            # Change project dir on each iteration
            project_name = "loop_test_project_" + str(i)
            prepare_msg = ServerMessage("prepare_execution", "1", json.dumps(project_name), ["helloworld.zip"])
            self.socket.send_multipart([prepare_msg.to_bytes(), file_data])
            prepare_response = self.socket.recv_multipart()
            prepare_response_msg = ServerMessage.parse(prepare_response[1])
            job_id = prepare_response_msg.getId()
            self.assertEqual("prepare_execution", prepare_response_msg.getCommand())
            # Send start_execution request
            start_msg = ServerMessage("start_execution", job_id, engine_data_json, None)
            self.socket.send_multipart([start_msg.to_bytes()])
            start_response = self.socket.recv_multipart()
            start_response_msg = ServerMessage.parse(start_response[1])
            start_response_msg_data = start_response_msg.getData()
            self.assertEqual("remote_execution_started", start_response_msg_data[0])
            self.receive_events(start_response_msg_data[1])
            self.service.kill_persistent_exec_mngrs()
            # TODO: Check that we use the same persistent exec manager in all 5 execution iterations

    def receive_events(self, publish_port):
        """Receives events from server until DAG execution has finished.

        Args:
            publish_port (str): Publish socket port
        """
        self.pull_socket.connect("tcp://localhost:" + publish_port)
        while True:
            socks = dict(self.poller.poll())
            if socks.get(self.pull_socket) == zmq.POLLIN:
                # Pull events
                rcv = self.pull_socket.recv_multipart()
                if len(rcv) > 1:  # Discard 'incoming_file', 'END' and file data messages
                    continue
                event_deconverted = EventDataConverter.deconvert(*rcv)
                if event_deconverted[0] == "dag_exec_finished":
                    self.assertEqual("COMPLETED", event_deconverted[1])
            if socks.get(self.socket) == zmq.POLLIN:
                # Wait for the 'completed' msg to make sure that the server can be closed
                resp = self.socket.recv_multipart()
                resp_msg = ServerMessage.parse(resp[1])
                resp_msg_data = resp_msg.getData()
                self.assertEqual("remote_execution_event", resp_msg_data[0])
                self.assertEqual("completed", resp_msg_data[1])
                break
        return

    def assert_error_response(self, expected_event_type, expected_start_of_error_msg):
        """Waits for a response from server and checks that the error msg is as expected."""
        response = self.socket.recv_multipart()
        server_msg = ServerMessage.parse(response[1])
        msg_data = server_msg.getData()
        self.assertEqual(expected_event_type, msg_data[0])
        self.assertTrue(msg_data[1].startswith(expected_start_of_error_msg))

    def make_default_item_dict(self, item_type):
        """Keep up-to-date with spinetoolbox.project_item.project_item.item_dict()."""
        return {
            "type": item_type,
            "description": "",
            "x": random.uniform(0, 100),
            "y": random.uniform(0, 100),
        }

    def make_dc_item_dict(self, file_ref=None):
        """Make a Data Connection item_dict.
        Keep up-to-date with spine_items.data_connection.data_connection.item_dict()."""
        d = self.make_default_item_dict("Data Connection")
        d["file_references"] = file_ref if file_ref is not None else list()
        d["db_references"] = []
        d["db_credentials"] = {}
        return d

    def make_tool_item_dict(self, spec_name, exec_in_work, options=None, group_id=None):
        """Make a Tool item_dict.
        Keep up-to-date with spine_items.tool.tool.item_dict()."""
        d = self.make_default_item_dict("Tool")
        d["specification"] = spec_name
        d["execute_in_work"] = exec_in_work
        d["cmd_line_args"] = []
        if options is not None:
            d["options"] = options
        if group_id is not None:
            d["group_id"] = group_id
        return d

    def make_importer_item_dict(self):
        d = self.make_default_item_dict("Importer")
        d["specification"] = "Importer 1 - units.xlsx"
        d["cancel_on_error"] = True
        d["on_conflict"] = "replace"
        d["file_selection"] = [["<Raw data>/units.xlsx", True]]
        return d

    def make_ds_item_dict(self):
        d = self.make_default_item_dict("Data Store")
        d["url"] = {"dialect": "sqlite", "username": "", "password": "", "host": "", "port": "", "database": {"type": "path", "relative": True, "path": ".spinetoolbox/items/ds1/DS1.sqlite"}}
        return d

    @staticmethod
    def make_python_tool_spec_dict(
            name,
            includes,
            input_files,
            exec_in_work,
            includes_main_path,
            def_file_path,
            exec_settings=None):
        return {
            "name": name,
            "tooltype": "python",
            "includes": includes,
            "description": "",
            "inputfiles": input_files,
            "inputfiles_opt": [],
            "outputfiles": [],
            "cmdline_args": [],
            "execute_in_work": exec_in_work,
            "includes_main_path": includes_main_path,
            "execution_settings": exec_settings if exec_settings is not None else dict(),
            "definition_file_path": def_file_path,
        }

    @staticmethod
    def make_importer_spec_dict():
        d = dict()
        d["name"] = "Importer 1 - units.xlsx"
        d["item_type"] = "Importer"
        d["mapping"] = {"table_mappings":
                            {"Sheet1":
                                 [
                                     {"": {"mapping": [{"map_type": "ObjectClass", "position": 0},
                                                       {"map_type": "Object", "position": 1},
                                                       {"map_type": "ObjectMetadata", "position": "hidden"}]}}
                                 ]
                            },
            "selected_tables": ["Sheet1"],
            "table_options": {"Sheet1": {}}, "table_types": {"Sheet1": {"0": "string", "1": "string"}},
            "table_default_column_type": {}, "table_row_types": {}, "source_type": "ExcelConnector"}
        d["description"] = "",
        d["definition_file_path"] = "C:\\data\\SpineToolboxData\\Projects\\Simple Importer\\Importer 1 - units.xlsx.json"
        return d

    def make_engine_data_for_helloworld_project(self):
        """Returns an engine data dictionary for SpineEngine() for the project in file helloworld.zip.

        engine_data dict must be the same as what is passed to SpineEngineWorker() in
        spinetoolbox.project.create_engine_worker()
        """
        tool_item_dict = self.make_tool_item_dict("Simple Tool Spec", False)
        dc_item_dict = self.make_dc_item_dict(file_ref=[{"type": "path", "relative": True, "path": "input_file.txt"}])
        spec_dict = self.make_python_tool_spec_dict(name="Simple Tool Spec",
                                                    includes=["simple_script.py"],
                                                    input_files=["input_file.txt"],
                                                    exec_in_work=True,
                                                    includes_main_path="../../..",
                                                    def_file_path="C:/data/temp/.spinetoolbox/specifications/Tool/simple_tool_spec.json",
                                                    exec_settings={"env": "", "kernel_spec_name": "py38", "use_jupyter_console": False, "executable": ""})
        item_dicts = dict()
        item_dicts["Data Connection 1"] = dc_item_dict
        item_dicts["Simple Tool"] = tool_item_dict
        specification_dicts = dict()
        specification_dicts["Tool"] = [spec_dict]
        engine_data = {
            "items": item_dicts,
            "specifications": specification_dicts,
            "connections": [{"name": "from Data Connection 1 to Simple Tool", "from": ["Data Connection 1", "right"], "to": ["Simple Tool", "left"]}],
            "jumps": [],
            "execution_permits": {"Data Connection 1": True, "Simple Tool": True},
            "items_module_name": "spine_items",
            "settings": {},
            "project_dir": "C:/data/temp",
        }
        return engine_data

    def make_engine_data_for_simple_importer_project(self):
        """Returns an engine data dictionary for SpineEngine() for the project in file simple_importer.zip.

        engine_data dict must be the same as what is passed to SpineEngineWorker() in
        spinetoolbox.project.create_engine_worker()
        """
        dc_item_dict = self.make_dc_item_dict()
        importer_item_dict = self.make_importer_item_dict()
        ds_item_dict = self.make_ds_item_dict()
        importer_spec_dict = self.make_importer_spec_dict()
        item_dicts = dict()
        item_dicts["Raw data"] = dc_item_dict
        item_dicts["Importer 1"] = importer_item_dict
        item_dicts["DS1"] = ds_item_dict
        specification_dicts = dict()
        specification_dicts["Importer"] = [importer_spec_dict]
        engine_data = {
            "items": item_dicts,
            "specifications": specification_dicts,
            "connections":  [{"name": "from Raw data to Importer 1", "from": ["Raw data", "right"], "to": ["Importer 1", "left"]}, {"name": "from Importer 1 to DS1", "from": ["Importer 1", "right"], "to": ["DS1", "left"], "options": {"purge_before_writing": True, "purge_settings": None}}],
            "jumps": [],
            "execution_permits": {"Importer 1": True, "DS1": True, "Raw data": True},
            "items_module_name": "spine_items",
            "settings": {},
            "project_dir": "C:/data/temp",
        }
        return engine_data


if __name__ == "__main__":
    unittest.main()
