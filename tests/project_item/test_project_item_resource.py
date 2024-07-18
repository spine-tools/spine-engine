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
""" Unit tests for ``project_item_resource`` module. """
from contextlib import ExitStack
from pathlib import Path
import sys
import unittest
from unittest import mock
from spine_engine.project_item.project_item_resource import (
    CmdLineArg,
    LabelArg,
    database_resource,
    expand_cmd_line_args,
    file_resource,
    file_resource_in_pack,
    get_labelled_source_resources,
    get_source,
    get_source_extras,
    labelled_resource_args,
    transient_file_resource,
    url_resource,
)
from spinedb_api import append_filter_config
from spinedb_api.filters.scenario_filter import scenario_filter_config
from spinedb_api.spine_db_server import db_server_manager


class TestGetLabelledSourceResources(unittest.TestCase):
    def test_empty_input_produces_empty_output(self):
        self.assertEqual(get_labelled_source_resources([]), {})

    def test_single_database_resource_gets_collected(self):
        resources = [database_resource("provider", "sqlite:///file.sqlite", "my database")]
        expected = {"my database": [resources[0]]}
        self.assertEqual(get_labelled_source_resources(resources), expected)

    def test_url_resource_gets_collected(self):
        resources = [url_resource("provider", "sqlite:///file.sqlite", "my non-Spine database")]
        expected = {"my non-Spine database": [resources[0]]}
        self.assertEqual(get_labelled_source_resources(resources), expected)

    def test_single_file_resource_gets_collected(self):
        resources = [file_resource("provider", "/path/to/file", "my file")]
        expected = {"my file": [resources[0]]}
        self.assertEqual(get_labelled_source_resources(resources), expected)

    def test_multiple_file_resources_in_packs_get_collected(self):
        resources = [
            file_resource_in_pack("provider", "*.dat", "/path/file1.dat"),
            file_resource_in_pack("provider", "*.dat", "/path/file2.dat"),
        ]
        expected = {"*.dat": [resources[0], resources[1]]}
        self.assertEqual(get_labelled_source_resources(resources), expected)


class TestLabelledResourceArgs(unittest.TestCase):
    def test_empty_resources(self):
        resources = []
        with ExitStack() as exit_stack:
            labelled_args = labelled_resource_args(resources, exit_stack)
        self.assertEqual(labelled_args, {})

    def test_single_resources(self):
        single_file_resource = file_resource("my provider", "/path/to/file")
        remote_resource = url_resource("so-called cloud", "ftp://ftp.sausage.org/wurst", "food-url")
        with db_server_manager() as db_server_manager_queue:
            db_resource = database_resource("db provider", "sqlite://")
            db_resource.metadata["db_server_manager_queue"] = db_server_manager_queue
            resources = [single_file_resource, db_resource, remote_resource]
            with ExitStack() as exit_stack:
                labelled_args = labelled_resource_args(resources, exit_stack)
            self.assertEqual(len(labelled_args), 3)
            self.assertEqual(labelled_args["/path/to/file"], [single_file_resource.path])
            self.assertEqual(len(labelled_args["sqlite://"]), 1)
            self.assertTrue(labelled_args["sqlite://"][0].startswith("http://127.0.0.1:"))
            self.assertEqual(labelled_args["food-url"], ["ftp://ftp.sausage.org/wurst"])

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


class TestDatabaseResource(unittest.TestCase):
    def test_schema_is_stored_in_metadata(self):
        resource = database_resource("project item", "sqlite:///path/to/db.sqlite", schema="my_schema")
        self.assertEqual(resource.metadata, {"schema": "my_schema"})

    def test_empty_label_is_replaced_by_url_without_filter_configs(self):
        url = "sqlite:///path/to/db.sqlite" if sys.platform == "win32" else "sqlite:////path/to/db.sqlite"
        filter_config = scenario_filter_config("my_scenario")
        filtered_url = append_filter_config(url, filter_config)
        self.assertNotEqual(url, filtered_url)
        resource = database_resource("project item", filtered_url)
        self.assertEqual(resource.url, filtered_url)
        self.assertEqual(resource.label, url)


class TestURLResource(unittest.TestCase):
    def test_schema_is_stored_in_metadata(self):
        resource = url_resource("project item", "sqlite:///path/do/db.sqlite", "my database", schema="my_schema")
        self.assertEqual(resource.metadata, {"schema": "my_schema"})

    def test_url_setter_works_when_url_is_set_to_none(self):
        resource = url_resource("project item", "sqlite:///path/do/db.sqlite", "my database")
        resource.url = None
        self.assertIsNone(resource.url)


class TestTransientFileResource(unittest.TestCase):
    def test_path_works_even_when_resource_has_no_url(self):
        resource = transient_file_resource("Provider", "files@Provider")
        self.assertEqual(resource.path, "")


class TestGetSource(unittest.TestCase):
    def test_file_resource(self):
        resource = file_resource("project item", "/path/to/file")
        self.assertEqual(get_source(resource), str(Path("/", "path", "to", "file").resolve()))

    def test_url_resource(self):
        resource = url_resource("project item", "mysql://user:psw@localhost/db", "database")
        self.assertEqual(get_source(resource), "mysql://user:psw@localhost/db")

    def test_database_resource(self):
        resource = database_resource("project item", "mysql://user:psw@localhost/db")
        self.assertEqual(get_source(resource), "mysql://user:psw@localhost/db")

    def test_transient_file_resource(self):
        resource = transient_file_resource("project item", "non-existent")
        self.assertIsNone(get_source(resource))


class TestGetSourceExtras(unittest.TestCase):
    def test_file_resource(self):
        resource = file_resource("project item", "/path/to/file")
        self.assertEqual(get_source_extras(resource), {})

    def test_url_resource(self):
        resource = url_resource("project item", "mysql://user:psw@localhost/db", "database", "myschema")
        self.assertEqual(get_source_extras(resource), {"schema": "myschema"})

    def test_database_resource(self):
        resource = database_resource("project item", "mysql://user:psw@localhost/db")
        self.assertEqual(get_source_extras(resource), {"schema": None})

    def test_transient_file_resource(self):
        resource = transient_file_resource("project item", "non-existent")
        self.assertEqual(get_source_extras(resource), {})


if __name__ == "__main__":
    unittest.main()
