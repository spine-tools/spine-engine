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
Unit tests for RemoteExecutionHandler class.
:author: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   24.8.2021
"""

import unittest
import zmq
import json
import os
import random
from pathlib import Path
from spine_engine.server.remote_execution_handler import RemoteExecutionHandler
from spine_engine.server.zmq_server import ZMQServer, ZMQSecurityModelState
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.util.server_message_parser import ServerMessageParser
from spine_engine.server.util.event_data_converter import EventDataConverter


class TestRemoteExecutionHandler(unittest.TestCase):
    def setUp(self):
        self.service = ZMQServer("tcp", 5559, ZMQSecurityModelState.NONE, "")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.identity = "Worker1".encode("ascii")
        self.socket.connect("tcp://localhost:5559")

    def tearDown(self):
        self.service.close()
        if not self.socket.closed:
            self.socket.close()
        if not self.context.closed:
            self.context.term()

    def test_remote_execution1(self):
        """Tests executing a DC -> Python Tool DAG."""
        engine_data = self.make_engine_data_for_test_zipfile_project()
        msg_data_json = json.dumps(engine_data)
        with open(os.path.join(str(Path(__file__).parent), "test_zipfile.zip"), "rb") as f:
            file_data = f.read()
        # File name used at server to save the transmitted project zip file. Does not need to be the same as original.
        zip_file_name = ["testi_zipu_file.zip"]
        msg = ServerMessage("execute", "1", msg_data_json, zip_file_name)
        part_one_bytes = bytes(msg.toJSON(), "utf-8")
        msg_parts = list()
        msg_parts.append(part_one_bytes)
        msg_parts.append(file_data)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msg_str = message.decode("utf-8")  # Decode received bytes to get (JSON) string
        parsed_msg = ServerMessageParser.parse(msg_str)  # Parse (JSON) string into a ServerMessage
        data = parsed_msg.getData()  # Get events+data in a dictionary
        data_events = EventDataConverter.convertJSON(data, True)
        self.assertEqual("dag_exec_finished", data_events[-1][0])
        self.assertEqual("COMPLETED", data_events[-1][1])

    def test_remote_execution2(self):
        """Tests executing a project with 3 items (1 Dc, 2 Tools, and 2 Tool specs)."""
        engine_data = self.make_engine_data_for_project_package_project()
        msg_data_json = json.dumps(engine_data)
        with open(os.path.join(str(Path(__file__).parent), "project_package.zip"), "rb") as f:
            data = f.read()
        msg = ServerMessage("execute", "1", msg_data_json, ["project_package.zip"])
        part_one_bytes = bytes(msg.toJSON(), "utf-8")
        msg_parts = list()
        msg_parts.append(part_one_bytes)
        msg_parts.append(data)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msgStr = message.decode("utf-8")
        parsedMsg = ServerMessageParser.parse(msgStr)
        data = parsedMsg.getData()
        data_events = EventDataConverter.convertJSON(data, True)
        self.assertEqual("dag_exec_finished", data_events[-1][0])
        self.assertEqual("COMPLETED", data_events[-1][1])

    def test_loop_calls(self):
        """Tests executing a DC -> Python Tool DAG five times in a row."""
        engine_data = self.make_engine_data_for_test_zipfile_project()
        with open(os.path.join(str(Path(__file__).parent), "test_zipfile.zip"), "rb") as f:
            data_file = f.read()
        i = 0
        while i < 5:
            # Switch project folder on each iteration
            engine_data["project_dir"] = "./helloworld" + str(i)
            engine_data["specifications"]["Tool"][0]["definition_file_path"] = (
                "./helloworld" + str(i) + "/.spinetoolbox/specifications/Tool/helloworld2.json"
            )
            msg_data_json = json.dumps(engine_data)
            msg = ServerMessage("execute", "1", msg_data_json, ["test_zipfile.zip"])
            msg_parts = list()
            msg_parts.append(msg.to_bytes())
            msg_parts.append(data_file)
            self.socket.send_multipart(msg_parts)
            message = self.socket.recv()
            msg_str = message.decode("utf-8")
            parsedMsg = ServerMessageParser.parse(msg_str)
            # get and decode events+data
            ret_data = parsedMsg.getData()
            dataEvents = EventDataConverter.convertJSON(ret_data, True)
            self.assertEqual(dataEvents[len(dataEvents) - 1][1], "COMPLETED")
            i += 1

    def test_invalid_project_folder(self):
        """Tests what happens when project_dir is an empty string."""
        engine_data = self.make_engine_data_for_test_zipfile_project()
        engine_data["project_dir"] = ""  # Clear project_dir for testing purposes
        msg_data_json = json.dumps(engine_data)
        with open(os.path.join(str(Path(__file__).parent), "test_zipfile.zip"), "rb") as f:
            data_file = f.read()
        msg = ServerMessage("execute", "1", msg_data_json, ["helloworld.zip"])
        msg_parts = list()
        msg_parts.append(msg.to_bytes())
        msg_parts.append(data_file)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msg_str = message.decode("utf-8")
        server_msg = ServerMessageParser.parse(msg_str)
        msg_data = server_msg.getData()
        self.assertTrue(msg_data.startswith("Problem in execute request."))

    def test_init_no_binarydata(self):
        """Try to execute a DC -> Tool DAG, but forget to send the project zip-file."""
        engine_data = self.make_engine_data_for_test_zipfile_project()
        msg_data_json = json.dumps(engine_data)
        msg = ServerMessage("execute", "1", msg_data_json, ["helloworld.zip"])
        msg_parts = list()
        msg_parts.append(msg.to_bytes())
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        response_str = message.decode("utf-8")
        server_msg = ServerMessageParser.parse(response_str)
        msg_data = server_msg.getData()
        self.assertTrue(msg_data.startswith("Project zip-file missing from request"))

    def test_no_filename(self):
        """Try to execute a DC->Tool DAG, but forget to include a zip-file name. """
        engine_data = self.make_engine_data_for_test_zipfile_project()
        msg_data_json = json.dumps(engine_data)
        with open(os.path.join(str(Path(__file__).parent), "test_zipfile.zip"), "rb") as f:
            data = f.read()
        msg = ServerMessage("execute", "1", msg_data_json, None)  # Note: files == None
        msg_parts = list()
        msg_parts.append(msg.to_bytes())
        msg_parts.append(data)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msgStr = message.decode("utf-8")
        server_msg = ServerMessageParser.parse(msgStr)
        msg_data = server_msg.getData()
        self.assertTrue(msg_data.startswith("Zip-file name missing"))

    def test_invalid_json(self):
        with open(os.path.join(str(Path(__file__).parent), "test_zipfile.zip"), "rb") as f2:
            data = f2.read()
        msg_data = '{"abc": "value:"123""}'  # Invalid JSON string (json.loads() should fail on server)
        msg = ServerMessage("execute", "1", msg_data, None)
        msg_parts = list()
        msg_parts.append(msg.to_bytes())
        msg_parts.append(data)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msg_str = message.decode("utf-8")
        server_msg = ServerMessageParser.parse(msg_str)
        ret_msg_data = server_msg.getData()
        self.assertTrue(ret_msg_data.startswith("json.decoder.JSONDecodeError:"))

    def test_path_for_local_project_dir(self):
        """In practice, given p for the tested method should always be an absolute path.
        Also, p is never None or an empty string (must be checked before calling this
        method)."""
        p = "./home/ubuntu/hellofolder"  # Linux relative
        ret = RemoteExecutionHandler.path_for_local_project_dir(p)
        _, dir_name = os.path.split(ret)
        self.assertTrue(os.path.isabs(ret))
        self.assertTrue(dir_name.startswith("hellofolder"))
        self.assertTrue(len(dir_name) == 45)  # e.g. "hellofolder__af724968e13b4fd782212921becafc47"
        p = "./hellofolder"  # Linux relative
        ret = RemoteExecutionHandler.path_for_local_project_dir(p)
        self.assertTrue(os.path.isabs(ret))
        self.assertTrue(dir_name.startswith("hellofolder"))
        self.assertTrue(len(dir_name) == 45)
        p = "/home/ubuntu/hellofolder"  # Linux absolute
        ret = RemoteExecutionHandler.path_for_local_project_dir(p)
        self.assertTrue(os.path.isabs(ret))
        self.assertTrue(dir_name.startswith("hellofolder"))
        self.assertTrue(len(dir_name) == 45)
        p = ".\\hellofolder"  # Windows relative
        ret = RemoteExecutionHandler.path_for_local_project_dir(p)
        self.assertTrue(os.path.isabs(ret))
        self.assertTrue(dir_name.startswith("hellofolder"))
        self.assertTrue(len(dir_name) == 45)
        p = "c:\\data\\project\\hellofolder"  # Windows absolute
        ret = RemoteExecutionHandler.path_for_local_project_dir(p)
        self.assertTrue(os.path.isabs(ret))
        self.assertTrue(dir_name.startswith("hellofolder"))
        self.assertTrue(len(dir_name) == 45)

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
        d["db_credentials"] = []
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

    @staticmethod
    def make_python_tool_spec_dict(
            name,
            script_path,
            input_file,
            exec_in_work,
            includes_main_path,
            def_file_path,
            exec_settings=None):
        return {
            "name": name,
            "tooltype": "python",
            "includes": script_path,
            "description": "",
            "inputfiles": input_file,
            "inputfiles_opt": [],
            "outputfiles": [],
            "cmdline_args": [],
            "execute_in_work": exec_in_work,
            "includes_main_path": includes_main_path,
            "execution_settings": exec_settings if exec_settings is not None else dict(),
            "definition_file_path": def_file_path,
        }

    def make_engine_data_for_test_zipfile_project(self):
        """Returns an engine data dictionary for SpineEngine() for the project in file test_zipfile.zip.

        engine_data dict must be the same as what is passed to SpineEngineWorker() in
        spinetoolbox.project.create_engine_worker()
        """
        tool_item_dict = self.make_tool_item_dict("helloworld2", False)
        dc_item_dict = self.make_dc_item_dict(file_ref=[{"type": "path", "relative": True, "path": "input2.txt"}])
        spec_dict = self.make_python_tool_spec_dict("helloworld2", ["helloworld.py"], ["input2.txt"], True, "../../..",
                                                    "./helloworld/.spinetoolbox/specifications/Tool/helloworld2.json")
        item_dicts = dict()
        item_dicts["helloworld"] = tool_item_dict
        item_dicts["Data Connection 1"] = dc_item_dict
        specification_dicts = dict()
        specification_dicts["Tool"] = [spec_dict]
        engine_data = {
            "items": item_dicts,
            "specifications": specification_dicts,
            "connections": [{"from": ["Data Connection 1", "left"], "to": ["helloworld", "right"]}],
            "jumps": [],
            "execution_permits": {"Data Connection 1": True, "helloworld": True},
            "items_module_name": "spine_items",
            "settings": {},
            "project_dir": "./helloworld",
        }
        return engine_data

    def make_engine_data_for_project_package_project(self):
        """Returns an engine data dictionary for SpineEngine() for the project in file project_package.zip.

        engine_data dict must be the same as what is passed to SpineEngineWorker() in
        spinetoolbox.project.create_engine_worker()
        """
        t1_item_dict = self.make_tool_item_dict("a", False)
        t2_item_dict = self.make_tool_item_dict("b", True,)
        dc1_item_dict = self.make_dc_item_dict()
        exec_settings_a = {
            "env": "",
            "kernel_spec_name": "python38",
            "use_jupyter_console": False,
            "executable": "",
            "fail_on_stderror": False
        }
        spec_dict_a = self.make_python_tool_spec_dict(
            "a",
            ["a.py"],
            [],
            True,
            ".",
            "C:/Users/ttepsa/OneDrive - Teknologian Tutkimuskeskus VTT/Documents/SpineToolboxProjects/remote test 3 items/.spinetoolbox/specifications/Tool/a.json",
            exec_settings_a
        )
        exec_settings_b = {
            "env": "",
            "kernel_spec_name": "python38",
            "use_jupyter_console": False,
            "executable": "",
            "fail_on_stderror": True
        }
        spec_dict_b = self.make_python_tool_spec_dict(
            "b",
            ["b.py"],
            [],
            True,
            "../../..",
            "C:/Users/ttepsa/OneDrive - Teknologian Tutkimuskeskus VTT/Documents/SpineToolboxProjects/remote test 3 items/.spinetoolbox/specifications/Tool/b.json",
            exec_settings_b
        )
        item_dicts = dict()
        item_dicts["T1"] = t1_item_dict
        item_dicts["T2"] = t2_item_dict
        item_dicts["DC1"] = dc1_item_dict
        specification_dicts = dict()
        specification_dicts["Tool"] = [spec_dict_a, spec_dict_b]
        engine_data = {
            "items": item_dicts,
            "specifications": specification_dicts,
            "connections": [{"from": ["DC1", "right"], "to": ["T1", "left"]},
                            {"from": ["T1", "right"], "to": ["T2", "left"]}],
            "jumps": [],
            "execution_permits": {"DC1": True, "T1": True, "T2": True},
            "items_module_name": "spine_items",
            "settings": {},
            "project_dir": "C:/Users/ttepsa/OneDrive - Teknologian Tutkimuskeskus VTT/Documents/SpineToolboxProjects/remote test 3 items",
        }
        return engine_data


if __name__ == "__main__":
    unittest.main()
