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
Unit tests for ServerMessage class.
"""

import json
import unittest
from spine_engine.server.util.server_message import ServerMessage


class TestServerMessage(unittest.TestCase):
    def make_engine_data1(self):
        engine_data = {
            "items": {
                "T1": {
                    "type": "Tool",
                    "description": "",
                    "x": -81.09856329675559,
                    "y": -7.8289158512399695,
                    "specification": "a",
                    "execute_in_work": True,
                    "cmd_line_args": [],
                },
                "DC1": {
                    "type": "Data Connection",
                    "description": "",
                    "x": -249.4143275244174,
                    "y": -122.2554094109619,
                    "file_references": [],
                    "db_references": [],
                    "db_credentials": {},
                },
            },
            "specifications": {
                "Tool": [
                    {
                        "name": "a",
                        "tooltype": "python",
                        "includes": ["a.py"],
                        "description": "",
                        "inputfiles": ["a.txt"],
                        "inputfiles_opt": [],
                        "outputfiles": [],
                        "cmdline_args": [],
                        "includes_main_path": ".",
                        "execution_settings": {
                            "env": "",
                            "kernel_spec_name": "python38",
                            "use_jupyter_console": False,
                            "executable": "",
                        },
                        "definition_file_path": "C:\\Users\\ttepsa\\OneDrive - Teknologian Tutkimuskeskus VTT\\Documents\\SpineToolboxProjects\\remote test 2 dags\\.spinetoolbox\\specifications\\Tool\\a.json",
                    }
                ]
            },
            "connections": [{"name": "from DC1 to T1", "from": ["DC1", "right"], "to": ["T1", "left"]}],
            "jumps": [],
            "execution_permits": {"T1": True, "DC1": True},
            "items_module_name": "spine_items",
            "settings": {
                "engineSettings/remoteExecutionEnabled": "true",
                "engineSettings/remoteHost": "192.168.56.69",
                "engineSettings/remotePort": 50001,
                "engineSettings/remoteSecurityFolder": "",
                "engineSettings/remoteSecurityModel": "",
            },
            "project_dir": "C:/Users/ttepsa/OneDrive - Teknologian Tutkimuskeskus VTT/Documents/SpineToolboxProjects/remote test 2 dags",
        }
        return engine_data

    def make_engine_data2(self):
        engine_data = {
            "items": {
                "Importer 1": {
                    "type": "Importer",
                    "description": "",
                    "x": 72.45758726309028,
                    "y": -80.20321040425301,
                    "specification": "Importer 1 - pekka_units.xlsx - 0",
                    "cancel_on_error": True,
                    "purge_before_writing": True,
                    "on_conflict": "replace",
                    "file_selection": [["<pekka data>/pekka_units.xlsx", True]],
                },
                "DS1": {
                    "type": "Data Store",
                    "description": "",
                    "x": 211.34193262411353,
                    "y": -152.99750295508272,
                    "url": {
                        "dialect": "sqlite",
                        "username": "",
                        "password": "",
                        "host": "",
                        "port": "",
                        "database": {"type": "path", "relative": True, "path": ".spinetoolbox/items/ds1/DS1.sqlite"},
                    },
                },
                "pekka data": {
                    "type": "Data Connection",
                    "description": "",
                    "x": -78.23453014184398,
                    "y": -145.98354018912534,
                    "file_references": [],
                    "db_references": [],
                    "db_credentials": {},
                },
            },
            "specifications": {
                "Importer": [
                    {
                        "name": "Importer 1 - pekka_units.xlsx - 0",
                        "item_type": "Importer",
                        "mapping": {
                            "table_mappings": {
                                "Sheet1": [
                                    {
                                        "map_type": "ObjectClass",
                                        "name": {"map_type": "column", "reference": 0},
                                        "parameters": {"map_type": "None"},
                                        "skip_columns": [],
                                        "read_start_row": 0,
                                        "objects": {"map_type": "column", "reference": 1},
                                    }
                                ]
                            },
                            "table_options": {},
                            "table_types": {"Sheet1": {"0": "string", "1": "string"}},
                            "table_row_types": {},
                            "selected_tables": ["Sheet1"],
                            "source_type": "ExcelConnector",
                        },
                        "description": None,
                        "definition_file_path": "C:\\Users\\ttepsa\\OneDrive - Teknologian Tutkimuskeskus VTT\\Documents\\SpineToolboxProjects\\Simple Importer\\Importer 1 - pekka_units.xlsx - 0.json",
                    }
                ]
            },
            "connections": [
                {
                    "name": "from pekka data to Importer 1",
                    "from": ["pekka data", "right"],
                    "to": ["Importer 1", "left"],
                },
                {"name": "from Importer 1 to DS1", "from": ["Importer 1", "right"], "to": ["DS1", "left"]},
            ],
            "jumps": [],
            "execution_permits": {"Importer 1": True, "DS1": True, "pekka data": True},
            "items_module_name": "spine_items",
            "settings": {
                "engineSettings/remoteExecutionEnabled": "true",
                "engineSettings/remoteHost": "192.168.56.69",
                "engineSettings/remotePort": 50001,
                "engineSettings/remoteSecurityFolder": "",
                "engineSettings/remoteSecurityModel": "",
            },
            "project_dir": "C:/Users/ttepsa/OneDrive - Teknologian Tutkimuskeskus VTT/Documents/SpineToolboxProjects/Simple Importer",
        }
        return engine_data

    def test_make_server_msg_and_parse1(self):
        """Engine data of DC -> Tool DAG"""
        engine_data = self.make_engine_data1()
        msg_data_json = json.dumps(engine_data)
        msg = ServerMessage("start_execution", "1", msg_data_json, ["not_needed.zip"])
        msg_as_bytes = msg.to_bytes()
        parsed_msg = ServerMessage.parse(msg_as_bytes)
        parsed_engine_data = parsed_msg.getData()
        self.assertEqual(engine_data, parsed_engine_data)

    def test_make_server_msg_and_parse2(self):
        """Engine data of DC -> Importer -> DS DAG"""
        engine_data = self.make_engine_data2()
        msg_data_json = json.dumps(engine_data)
        msg = ServerMessage("start_execution", "1", msg_data_json, ["not_needed.zip"])
        msg_as_bytes = msg.to_bytes()
        parsed_msg = ServerMessage.parse(msg_as_bytes)
        parsed_engine_data = parsed_msg.getData()
        self.assertEqual(engine_data, parsed_engine_data)

    def test_msg_creation(self):
        list_files = ["dffd.zip", "fdeef.zip"]
        msg = ServerMessage("execute", "4", "[]", list_files)
        self.assertEqual("execute", msg.getCommand())
        self.assertEqual("4", msg.getId())
        self.assertEqual(list_files, msg.getFileNames())
        self.assertEqual("[]", msg.getData())

    def test_msg_creation_nofilenames(self):
        msg = ServerMessage("execute", "4", "[]", None)
        self.assertEqual("execute", msg.getCommand())
        self.assertEqual("4", msg.getId())
        self.assertEqual("[]", msg.getData())

    def test_msg_nodata(self):
        listFiles = ["dffd.zip", "fdeef.zip"]
        msg = ServerMessage("execute", "4", "", listFiles)
        self.assertEqual("execute", msg.getCommand())
        self.assertEqual("4", msg.getId())

    def test_invalid_input1(self):
        with self.assertRaises(ValueError):  # json.decoder.JSONDecodeError is a ValueError
            ServerMessage.parse(b"")

    def test_invalid_input2(self):
        with self.assertRaises(ValueError):
            ServerMessage.parse(b"fiuoehfoiewjkfdewjiofdj{")

    def test_msg_nofiles(self):
        msg = ServerMessage("execute", "4", "{}", None)
        jsonStr = msg.to_bytes()
        msg = ServerMessage.parse(jsonStr)
        self.assertEqual(msg.getCommand(), "execute")
        self.assertEqual(msg.getId(), "4")
        self.assertEqual(msg.getData(), {})
        self.assertEqual(len(msg.getFileNames()), 0)


if __name__ == "__main__":
    unittest.main()
