######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""
Unit tests for ``project_item_resource`` module.

:authors: A. Soininen
:date:    8.8.2022
"""
from contextlib import ExitStack
from pathlib import Path
import unittest
from unittest import mock
from spine_engine.project_item.project_item_resource import (
    CmdLineArg,
    database_resource,
    expand_cmd_line_args,
    file_resource,
    file_resource_in_pack,
    get_labelled_sources,
    LabelArg,
    labelled_resource_args,
)
from spinedb_api.spine_db_server import db_server_manager


class TestGetLabelledSources(unittest.TestCase):
    def test_empty_input_produces_empty_output(self):
        self.assertEqual(get_labelled_sources([]), {})

    def test_single_database_resource_gets_collected(self):
        resources = [database_resource("provider", "sqlite:///file.sqlite", "my database")]
        expected = {"my database": ["sqlite:///file.sqlite"]}
        self.assertEqual(get_labelled_sources(resources), expected)

    def test_single_file_resource_gets_collected(self):
        resources = [file_resource("provider", "/path/to/file", "my file")]
        expected = {"my file": [str(Path("/", "path", "to", "file").resolve())]}
        self.assertEqual(get_labelled_sources(resources), expected)

    def test_multiple_file_resources_in_packs_get_collected(self):
        resources = [
            file_resource_in_pack("provider", "*.dat", "/path/file1.dat"),
            file_resource_in_pack("provider", "*.dat", "/path/file2.dat"),
        ]
        expected = {
            "*.dat": [str(Path("/", "path", "file1.dat").resolve()), str(Path("/", "path", "file2.dat").resolve())]
        }
        self.assertEqual(get_labelled_sources(resources), expected)


class TestLabelledResourceArgs(unittest.TestCase):
    def test_empty_resources(self):
        resources = []
        with ExitStack() as exit_stack:
            labelled_args = labelled_resource_args(resources, exit_stack)
        self.assertEqual(labelled_args, {})

    def test_single_resources(self):
        single_file_resource = file_resource("my provider", "/path/to/file")
        with db_server_manager() as db_server_manager_queue:
            db_resource = database_resource("db provider", "sqlite://")
            db_resource.metadata["db_server_manager_queue"] = db_server_manager_queue
            resources = [single_file_resource, db_resource]
            with ExitStack() as exit_stack:
                labelled_args = labelled_resource_args(resources, exit_stack)
            self.assertEqual(len(labelled_args), 2)
            self.assertEqual(labelled_args["/path/to/file"], [single_file_resource.path])
            self.assertEqual(len(labelled_args["sqlite://"]), 1)
            self.assertTrue(labelled_args["sqlite://"][0].startswith("http://127.0.0.1:"))

    def test_pack_resource(self):
        file_1 = file_resource_in_pack("my provider", "pack", "/path/to/file1")
        file_2 = file_resource_in_pack("my provider", "pack", "/path/to/file2")
        resources = [file_1, file_2]
        with ExitStack() as exit_stack:
            labelled_args = labelled_resource_args(resources, exit_stack)
        self.assertEqual(labelled_args, {"pack": [file_1.path, file_2.path]})


class TestExpandCmdLineArgs(unittest.TestCase):
    def setUp(self):
        self._logger = mock.MagicMock()

    def test_empty_args(self):
        self.assertEqual(expand_cmd_line_args([], {}, self._logger), [])

    def test_single_label_arg(self):
        args = [LabelArg("file label")]
        label_to_arg = {"file label": ["/path/to/file"]}
        expanded_args = expand_cmd_line_args(args, label_to_arg, self._logger)
        self.assertEqual(expanded_args, ["/path/to/file"])

    def test_multiple_args_for_label(self):
        args = [LabelArg("file label")]
        label_to_arg = {"file label": ["/path/to/file1", "/path/to/file2"]}
        expanded_args = expand_cmd_line_args(args, label_to_arg, self._logger)
        self.assertEqual(expanded_args, ["/path/to/file1", "/path/to/file2"])

    def test_non_label_arg(self):
        args = [CmdLineArg("--no-worries")]
        label_to_arg = {"--no-worries": ["/path/to/file1", "/path/to/file2"]}
        expanded_args = expand_cmd_line_args(args, label_to_arg, self._logger)
        self.assertEqual(expanded_args, ["--no-worries"])


if __name__ == '__main__':
    unittest.main()
