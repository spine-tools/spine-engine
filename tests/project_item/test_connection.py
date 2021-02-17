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
Uni tests for the ``connection`` module.

:authors: A. Soininen (VTT)
:date:    18.2.2021
"""
import os.path
from tempfile import TemporaryDirectory
import unittest
from spinedb_api import DiffDatabaseMapping, import_scenarios, import_tools
from spine_engine.project_item.connection import Connection
from spine_engine.project_item.project_item_resource import ProjectItemResource


class TestConnection(unittest.TestCase):
    def test_serialization_without_filters(self):
        connection = Connection("source", "bottom", "destination", "top")
        connection_dict = connection.to_dict()
        restored = Connection.from_dict(connection_dict)
        self.assertEqual(restored.source, "source")
        self.assertEqual(restored.source_position, "bottom")
        self.assertEqual(restored.destination, "destination")
        self.assertEqual(restored.destination_position, "top")
        self.assertFalse(restored.has_filters())

    def test_serialization_with_filters(self):
        filters = {"label": {"scenario_filter": {13: True}}}
        connection = Connection("source", "bottom", "destination", "top", filters)
        connection_dict = connection.to_dict()
        restored = Connection.from_dict(connection_dict)
        self.assertEqual(restored.source, "source")
        self.assertEqual(restored.source_position, "bottom")
        self.assertEqual(restored.destination, "destination")
        self.assertEqual(restored.destination_position, "top")
        self.assertTrue(restored.has_filters())
        self.assertEqual(restored.resource_filters, filters)

    def test_set_online(self):
        filters = {"label": {"scenario_filter": {13: False}}}
        connection = Connection("source", "bottom", "destination", "top", filters)
        connection.set_online("label", "scenario_filter", {13: True})
        self.assertEqual(connection.resource_filters, {"label": {"scenario_filter": {13: True}}})


class TestConnectionWithDatabase(unittest.TestCase):
    def setUp(self):
        self._temp_dir = TemporaryDirectory()

    def tearDown(self):
        self._temp_dir.cleanup()

    def test_fetch_scenarios(self):
        connection = Connection("source", "bottom", "destination", "top")
        url = "sqlite:///" + os.path.join(self._temp_dir.name, "db.sqlite")
        db_map = DiffDatabaseMapping(url, create=True)
        import_scenarios(db_map, ("scenario",))
        db_map.commit_session("Add test data.")
        db_map.connection.close()
        resources = [ProjectItemResource(_NamedObject("source"), "database", url)]
        connection.receive_resources_from_source(resources)
        self.assertFalse(connection.has_filters())
        connection.fetch_database_items()
        self.assertTrue(connection.has_filters())
        self.assertEqual(connection.resource_filters, {url: {"scenario_filter": {1: False}}})

    def test_fetch_tools(self):
        connection = Connection("source", "bottom", "destination", "top")
        url = "sqlite:///" + os.path.join(self._temp_dir.name, "db.sqlite")
        db_map = DiffDatabaseMapping(url, create=True)
        import_tools(db_map, ("tool",))
        db_map.commit_session("Add test data.")
        db_map.connection.close()
        resources = [ProjectItemResource(_NamedObject("source"), "database", url)]
        connection.receive_resources_from_source(resources)
        self.assertFalse(connection.has_filters())
        connection.fetch_database_items()
        self.assertTrue(connection.has_filters())
        self.assertEqual(connection.resource_filters, {url: {"tool_filter": {1: False}}})


class _NamedObject:
    def __init__(self, name):
        self.name = name


if __name__ == "__main__":
    unittest.main()
