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
Unit tests for RemoteConnectionHandler class.
:author: P. Pääkkönen (VTT)
:date:   24.8.2021
"""

import unittest
import zmq
import json
import os
from pathlib import Path
from spine_engine.server.remote_connection_handler import RemoteConnectionHandler
from spine_engine.server.connectivity.zmq_server import ZMQSecurityModelState
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.util.server_message_parser import ServerMessageParser
from spine_engine.server.util.event_data_converter import EventDataConverter
from spine_engine.server.start_server import RemoteSpineService


class TestRemoteConnectionHandler(unittest.TestCase):
    def setUp(self):
        self.service = RemoteSpineService("tcp", 5559, ZMQSecurityModelState.NONE, "")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://localhost:5559")

    def tearDown(self):
        self.service.close()
        if not self.socket.closed:
            self.socket.close()
        if not self.context.closed:
            self.context.term()

    @staticmethod
    def _dict_data(
        items,
        connections,
        node_successors,
        execution_permits,
        specifications,
        settings,
        project_dir,
        jumps,
        items_module_name,
    ):
        """Returns a dict to be passed to the class.

        Args:
            items (list(dict)): See SpineEngine.__init()
            connections (list of dict): See SpineEngine.__init()
            node_successors (dict(str,list(str))): See SpineEngine.__init()
            execution_permits (dict(str,bool)): See SpineEngine.__init()
            specifications (dict(str,list(dict))): SpineEngine.__init()
            settings (dict): SpineEngine.__init()
            project_dir (str): SpineEngine.__init()
            jumps (List of jump dicts): SpineEngine.__init()
            items_module_name (str): SpineEngine.__init()

        Returns:
            dict: Some data
        """
        item = dict()
        item["items"] = items
        item["specifications"] = specifications
        item["connections"] = connections
        item["jumps"] = jumps
        item["node_successors"] = node_successors
        item["execution_permits"] = execution_permits
        item["items_module_name"] = items_module_name
        item["settings"] = settings
        item["project_dir"] = project_dir
        return item

    def test_init_complete(self):
        msg_parts = []
        dict_data2 = {
            "items": {
                "helloworld": {
                    "type": "Tool",
                    "description": "",
                    "x": -91.6640625,
                    "y": -5.609375,
                    "specification": "helloworld2",
                    "execute_in_work": False,
                    "cmd_line_args": [],
                },
                "Data Connection 1": {
                    "type": "Data Connection",
                    "description": "",
                    "x": 62.7109375,
                    "y": 8.609375,
                    "references": [{"type": "path", "relative": True, "path": "input2.txt"}],
                },
            },
            "connections": [{"from": ["Data Connection 1", "left"], "to": ["helloworld", "right"]}],
            "node_successors": {"Data Connection 1": ["helloworld"], "helloworld": []},
            "execution_permits": {"Data Connection 1": True, "helloworld": True},
            "project_dir": "./helloworld",
            "specifications": {
                "Tool": [
                    {
                        "name": "helloworld2",
                        "tooltype": "python",
                        "includes": ["helloworld.py"],
                        "description": "",
                        "inputfiles": ["input2.txt"],
                        "inputfiles_opt": [],
                        "outputfiles": [],
                        "cmdline_args": [],
                        "execute_in_work": True,
                        "includes_main_path": "../../..",
                        "definition_file_path": "./helloworld/.spinetoolbox/specifications/Tool/helloworld2.json",
                    }
                ]
            },
            "settings": {},
            "jumps": [],
            "items_module_name": "spine_items",
        }
        msgDataJson = json.dumps(dict_data2)
        f = open(os.path.join(str(Path(__file__).parent), "test_zipfile.zip"), "rb")
        data = f.read()
        f.close()
        listFiles = ["helloworld.zip"]
        msg = ServerMessage("execute", "1", msgDataJson, listFiles)
        part1Bytes = bytes(msg.toJSON(), "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(data)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msgStr = message.decode("utf-8")
        parsedMsg = ServerMessageParser.parse(msgStr)
        # get and decode events+data
        data = parsedMsg.getData()
        jsonData = json.dumps(data)
        dataEvents = EventDataConverter.convertJSON(jsonData, True)
        self.assertEqual(dataEvents[len(dataEvents) - 1][1], "COMPLETED")

    def test_init_complete2(self):
        """Tests unzipping and executing a project with 3 items (1 Dc, 2 Tools)."""
        msg_parts = []
        engine_data = {
            "items": {
                "T1": {
                    "type": "Tool",
                    "description": "",
                    "x": -12.991220992469263,
                    "y": 40.624746917245005,
                    "specification": "a",
                    "execute_in_work": False,
                    "cmd_line_args": [],
                },
                "T2": {
                    "type": "Tool",
                    "description": "",
                    "x": 109.32691674373322,
                    "y": -36.124746917245005,
                    "specification": "b",
                    "execute_in_work": True,
                    "cmd_line_args": [],
                },
                "DC1": {
                    "type": "Data Connection",
                    "description": "",
                    "x": -124.82691674373321,
                    "y": -33.45884058790706,
                    "references": [],
                },
            },
            "specifications": {
                "Tool": [
                    {
                        "name": "a",
                        "tooltype": "python",
                        "includes": ["a.py"],
                        "description": "",
                        "inputfiles": [],
                        "inputfiles_opt": [],
                        "outputfiles": [],
                        "cmdline_args": [],
                        "execute_in_work": False,
                        "includes_main_path": ".",
                        "execution_settings": {
                            "env": "",
                            "kernel_spec_name": "python38",
                            "use_jupyter_console": False,
                            "executable": "",
                            "fail_on_stderror": False
                        },
                        "definition_file_path": "C:/Users/ttepsa/OneDrive - Teknologian Tutkimuskeskus VTT/Documents/SpineToolboxProjects/remote test 3 items/.spinetoolbox/specifications/Tool/a.json"
                    },
                    {
                        "name": "b",
                        "tooltype": "python",
                        "includes": ["b.py"],
                        "description": "",
                        "inputfiles": [],
                        "inputfiles_opt": [],
                        "outputfiles": [],
                        "cmdline_args": [],
                        "execute_in_work": True,
                        "includes_main_path": "../../..",
                        "execution_settings": {
                            "env": "",
                            "kernel_spec_name": "python38",
                            "use_jupyter_console": False,
                            "executable": "",
                            "fail_on_stderror": True
                        },
                        "definition_file_path": "C:/Users/ttepsa/OneDrive - Teknologian Tutkimuskeskus VTT/Documents/SpineToolboxProjects/remote test 3 items/.spinetoolbox/specifications/Tool/b.json"}
                ],
            },
            "connections":
                [
                    {"from": ["DC1", "right"], "to": ["T1", "left"]},
                    {"from": ["T1", "right"], "to": ["T2", "left"]}
                ],
            "jumps": [],
            "node_successors": {"DC1": ["T1"], "T1": ["T2"], "T2": []},
            "execution_permits":
                {
                    "DC1": True, "T1": True, "T2": True
                },
            "items_module_name": "spine_items",
            "settings": {},
            "project_dir": "C:/Users/ttepsa/OneDrive - Teknologian Tutkimuskeskus VTT/Documents/SpineToolboxProjects/remote test 3 items"
        }
        msgDataJson = json.dumps(engine_data)
        with open(os.path.join(str(Path(__file__).parent), "project_package.zip"), "rb") as f:
            data = f.read()
        listFiles = ["project_package.zip"]  # optional
        msg = ServerMessage("execute", "1", msgDataJson, listFiles)
        part1Bytes = bytes(msg.toJSON(), "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(data)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msgStr = message.decode("utf-8")
        parsedMsg = ServerMessageParser.parse(msgStr)
        data = parsedMsg.getData()
        jsonData = json.dumps(data)
        dataEvents = EventDataConverter.convertJSON(jsonData, True)
        self.assertEqual(dataEvents[len(dataEvents) - 1][1], "COMPLETED")

    def test_invalid_project_folder(self):
        """project_dir is an empty string."""
        msg_parts = []
        dict_data2 = {
            "items": {
                "helloworld": {
                    "type": "Tool",
                    "description": "",
                    "x": -91.6640625,
                    "y": -5.609375,
                    "specification": "helloworld2",
                    "execute_in_work": False,
                    "cmd_line_args": [],
                },
                "Data Connection 1": {
                    "type": "Data Connection",
                    "description": "",
                    "x": 62.7109375,
                    "y": 8.609375,
                    "references": [{"type": "path", "relative": True, "path": "input2.txt"}],
                },
            },
            "connections": [{"from": ["Data Connection 1", "left"], "to": ["helloworld", "right"]}],
            "node_successors": {"Data Connection 1": ["helloworld"], "helloworld": []},
            "execution_permits": {"Data Connection 1": True, "helloworld": True},
            "project_dir": "",
            "specifications": {
                "Tool": [
                    {
                        "name": "helloworld2",
                        "tooltype": "python",
                        "includes": ["helloworld.py"],
                        "description": "",
                        "inputfiles": ["input2.txt"],
                        "inputfiles_opt": [],
                        "outputfiles": [],
                        "cmdline_args": [],
                        "execute_in_work": True,
                        "includes_main_path": "../../..",
                        "definition_file_path": "./helloworld/.spinetoolbox/specifications/Tool/helloworld2.json",
                    }
                ]
            },
            "settings": {},
            "jumps": [],
            "items_module_name": "spine_items",
        }
        msgDataJson = json.dumps(dict_data2)
        with open(os.path.join(str(Path(__file__).parent), "test_zipfile.zip"), "rb") as f:
            data = f.read()
        listFiles = ["helloworld.zip"]
        msg = ServerMessage("execute", "1", msgDataJson, listFiles)
        part1Bytes = bytes(msg.toJSON(), "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(data)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msgStr = message.decode("utf-8")
        server_msg = ServerMessageParser.parse(msgStr)
        msg_data = server_msg.getData()
        self.assertTrue(msg_data.startswith("Problem in execute request."))

    def test_loop_calls(self):
        dict_data2 = {
            "items": {
                "helloworld": {
                    "type": "Tool",
                    "description": "",
                    "x": -91.6640625,
                    "y": -5.609375,
                    "specification": "helloworld2",
                    "execute_in_work": False,
                    "cmd_line_args": [],
                },
                "Data Connection 1": {
                    "type": "Data Connection",
                    "description": "",
                    "x": 62.7109375,
                    "y": 8.609375,
                    "references": [{"type": "path", "relative": True, "path": "input2.txt"}],
                },
            },
            "connections": [{"from": ["Data Connection 1", "left"], "to": ["helloworld", "right"]}],
            "node_successors": {"Data Connection 1": ["helloworld"], "helloworld": []},
            "execution_permits": {"Data Connection 1": True, "helloworld": True},
            "project_dir": "./helloworld2",
            "specifications": {
                "Tool": [
                    {
                        "name": "helloworld2",
                        "tooltype": "python",
                        "includes": ["helloworld.py"],
                        "description": "",
                        "inputfiles": ["input2.txt"],
                        "inputfiles_opt": [],
                        "outputfiles": [],
                        "cmdline_args": [],
                        "execute_in_work": True,
                        "includes_main_path": "../../..",
                        "definition_file_path": "./helloworld/.spinetoolbox/specifications/Tool/helloworld2.json",
                    }
                ]
            },
            "settings": {},
            "jumps": [],
            "items_module_name": "spine_items",
        }
        with open(os.path.join(str(Path(__file__).parent), "test_zipfile.zip"), "rb") as f:
            data = f.read()
        listFiles = ["helloworld.zip"]
        i = 0
        while i < 2:  # TODO: Works with "while i < 2". does not work with "while i < 3". Problem may be reaching the high-water mark?!
            msg_parts = []
            dict_data2["project_dir"] = "./helloworld" + str(i)
            dict_data2["specifications"]["Tool"][0]["definition_file_path"] = (
                "./helloworld" + str(i) + "/.spinetoolbox/specifications/Tool/helloworld2.json"
            )
            msgDataJson = json.dumps(dict_data2)
            msg = ServerMessage("execute", "1", msgDataJson, listFiles)
            part1Bytes = bytes(msg.toJSON(), "utf-8")
            msg_parts.append(part1Bytes)
            msg_parts.append(data)
            send_ret = self.socket.send_multipart(msg_parts, track=True)
            # print("test_loop_calls(): listening to replies..%d"%i)
            message = self.socket.recv()
            msgStr = message.decode("utf-8")
            # print("out recv()..Received reply %s" %msgStr)
            parsedMsg = ServerMessageParser.parse(msgStr)
            # get and decode events+data
            retData = parsedMsg.getData()
            jsonData = json.dumps(retData)
            dataEvents = EventDataConverter.convertJSON(jsonData, True)
            self.assertEqual(dataEvents[len(dataEvents) - 1][1], "COMPLETED")
            i += 1

    def test_init_no_binarydata(self):
        """Send message with JSON, but no binary data."""
        msg_parts = []
        f = open(os.path.join(str(Path(__file__).parent), "msg_data1.txt"))
        msgData = f.read()
        f.close()
        msgDataJson = json.dumps(msgData)
        listFiles = ["helloworld.zip"]
        msg = ServerMessage("execute", "1", msgDataJson, listFiles)
        part1Bytes = bytes(msg.toJSON(), "utf-8")
        msg_parts.append(part1Bytes)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msgStr = message.decode("utf-8")
        server_msg = ServerMessageParser.parse(msgStr)
        msg_data = server_msg.getData()
        self.assertTrue(msg_data.startswith("Message should have two parts."))

    def test_no_filename(self):
        msg_parts = []
        f = open(os.path.join(str(Path(__file__).parent), "msg_data1.txt"))
        msgData = f.read()
        f.close()
        msgDataJson = json.dumps(msgData)
        with open(os.path.join(str(Path(__file__).parent), "test_zipfile.zip"), "rb") as f:
            data = f.read()
        msg = ServerMessage("execute", "1", msgDataJson, None)
        part1Bytes = bytes(msg.toJSON(), "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(data)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msgStr = message.decode("utf-8")
        server_msg = ServerMessageParser.parse(msgStr)
        msg_data = server_msg.getData()
        self.assertTrue(msg_data.startswith("Zip-file name missing"))

    def test_invalid_json(self):
        msg_parts = []
        with open(os.path.join(str(Path(__file__).parent), "msg_data2.txt")) as f:
            msgData = f.read()
        with open(os.path.join(str(Path(__file__).parent), "test_zipfile.zip"), "rb") as f2:
            data = f2.read()
        msg = ServerMessage("execute", "1", msgData, None)
        part1Bytes = bytes(msg.toJSON(), "utf-8")
        msg_parts.append(part1Bytes)
        msg_parts.append(data)
        self.socket.send_multipart(msg_parts)
        message = self.socket.recv()
        msgStr = message.decode("utf-8")
        server_msg = ServerMessageParser.parse(msgStr)
        msg_data = server_msg.getData()
        self.assertTrue(msg_data.startswith("JSONDecodeError:"))

    def test_path_for_local_project_dir(self):
        """In practice, given p for the tested method should always be an absolute path.
        Also, p is never None or an empty string (must be checked before calling this
        method)."""
        p = "./home/ubuntu/hellofolder"  # Linux relative
        ret = RemoteConnectionHandler.path_for_local_project_dir(p)
        _, dir_name = os.path.split(ret)
        self.assertTrue(os.path.isabs(ret))
        self.assertTrue(dir_name.startswith("hellofolder"))
        self.assertTrue(len(dir_name) == 45)  # e.g. "hellofolder__af724968e13b4fd782212921becafc47"
        p = "./hellofolder"  # Linux relative
        ret = RemoteConnectionHandler.path_for_local_project_dir(p)
        self.assertTrue(os.path.isabs(ret))
        self.assertTrue(dir_name.startswith("hellofolder"))
        self.assertTrue(len(dir_name) == 45)
        p = "/home/ubuntu/hellofolder"  # Linux absolute
        ret = RemoteConnectionHandler.path_for_local_project_dir(p)
        self.assertTrue(os.path.isabs(ret))
        self.assertTrue(dir_name.startswith("hellofolder"))
        self.assertTrue(len(dir_name) == 45)
        p = ".\\hellofolder"  # Windows relative
        ret = RemoteConnectionHandler.path_for_local_project_dir(p)
        self.assertTrue(os.path.isabs(ret))
        self.assertTrue(dir_name.startswith("hellofolder"))
        self.assertTrue(len(dir_name) == 45)
        p = "c:\\data\\project\\hellofolder"  # Windows absolute
        ret = RemoteConnectionHandler.path_for_local_project_dir(p)
        self.assertTrue(os.path.isabs(ret))
        self.assertTrue(dir_name.startswith("hellofolder"))
        self.assertTrue(len(dir_name) == 45)


if __name__ == "__main__":
    unittest.main()
