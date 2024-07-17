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

"""Unit tests for chunk module."""
import sys
import unittest
from unittest import mock
from spine_engine.project_item.connection import Connection, FilterSettings
from spine_engine.utils.helpers import (
    gather_leaf_data,
    get_file_size,
    make_dag,
    required_items_for_execution,
    resolve_python_interpreter,
)
from spinedb_api.filters.scenario_filter import SCENARIO_FILTER_TYPE
from spinedb_api.helpers import remove_credentials_from_url


class TestRequiredItemsForExecution(unittest.TestCase):
    class NormalItem:
        @staticmethod
        def is_filter_terminus():
            return False

    class TerminusItem:
        @staticmethod
        def is_filter_terminus():
            return True

    _item_classes = {
        "Normal": NormalItem,
        "Terminus": TerminusItem,
    }

    def test_single_permitted_item(self):
        items = {"item a": {"type": "Normal"}}
        connections = []
        permits = {"item a": True}
        items = required_items_for_execution(items, connections, self._item_classes, permits)
        self.assertEqual(items, {"item a"})

    def test_two_connected_items_last_one_permitted(self):
        items = {"item a": {"type": "Normal"}, "item b": {"type": "Normal"}}
        connections = [Connection("item a", "right", "item b", "left")]
        permits = {"item b": True}
        items = required_items_for_execution(items, connections, self._item_classes, permits)
        self.assertEqual(items, {"item b"})

    def test_filtered_chain_last_item_permitted(self):
        items = {"item a": {"type": "Normal"}, "item b": {"type": "Normal"}, "item c": {"type": "Normal"}}
        filter_settings = FilterSettings({"resource@a": {SCENARIO_FILTER_TYPE: {"filter 1": True}}})
        connections = [
            Connection("item a", "right", "item b", "left", filter_settings=filter_settings),
            Connection("item b", "right", "item c", "left"),
        ]
        permits = {"item c": True}
        items = required_items_for_execution(items, connections, self._item_classes, permits)
        self.assertEqual(items, {"item b", "item c"})

    def test_filter_terminus_ends_filtered_fork(self):
        items = {"item a": {"type": "Normal"}, "item b": {"type": "Terminus"}, "item c": {"type": "Normal"}}
        filter_settings = FilterSettings({"resource@a": {SCENARIO_FILTER_TYPE: {"filter 1": True}}})
        connections = [
            Connection("item a", "right", "item b", "left", filter_settings=filter_settings),
            Connection("item b", "right", "item c", "left"),
        ]
        permits = {"item c": True}
        items = required_items_for_execution(items, connections, self._item_classes, permits)
        self.assertEqual(items, {"item c"})

    def test_filter_terminus_ends_filtered_fork_with_longer_chain(self):
        items = {
            "item a": {"type": "Normal"},
            "item b": {"type": "Normal"},
            "item c": {"type": "Terminus", "item d": {"type": "Normal"}},
        }
        filter_settings = FilterSettings({"resource@a": {SCENARIO_FILTER_TYPE: {"filter 1": True}}})
        connections = [
            Connection("item a", "right", "item b", "left", filter_settings=filter_settings),
            Connection("item b", "right", "item c", "left"),
            Connection("item c", "right", "item d", "left"),
        ]
        permits = {"item d": True}
        items = required_items_for_execution(items, connections, self._item_classes, permits)
        self.assertEqual(items, {"item d"})


class TestMakeDAG(unittest.TestCase):
    def test_single_node(self):
        edges = {"a": None}
        dag = make_dag(edges)
        self.assertEqual(len(dag), 1)

    def test_two_nodes(self):
        edges = {"a": ["b"], "b": None}
        dag = make_dag(edges)
        self.assertEqual(set(dag.nodes), {"a", "b"})
        self.assertEqual(list(dag.edges), [("a", "b")])

    def test_branch(self):
        edges = {"a": ["b", "c"], "b": None, "c": None}
        dag = make_dag(edges)
        self.assertEqual(set(dag.nodes), {"a", "b", "c"})
        self.assertEqual(set(dag.edges), {("a", "b"), ("a", "c")})

    def test_no_edges_one_node(self):
        edges = {}
        permitted_nodes = {"a": True}
        dag = make_dag(edges, permitted_nodes)
        self.assertEqual(len(dag), 1)

    def test_no_edges_two_nodes(self):
        # Note: This does not happen in practice because unconnected nodes will be executed on different Engines
        edges = {}
        permitted_nodes = {"a": True, "b": True}
        dag = make_dag(edges, permitted_nodes)
        self.assertEqual(len(dag), 2)


class TestRemoveCredentialsFromUrl(unittest.TestCase):
    def test_no_credentials_in_url(self):
        no_credentials = r"sqlite:///C:\data\db.sqlite"
        self.assertEqual(remove_credentials_from_url(no_credentials), no_credentials)

    def test_credentials_in_database_url(self):
        with_credentials = "mysql+pymysql://username:password@remote.fi/database"
        self.assertEqual(remove_credentials_from_url(with_credentials), "mysql+pymysql://remote.fi/database")


class TestGatherLeafData(unittest.TestCase):
    def test_empty_inputs_give_empty_output(self):
        self.assertEqual(gather_leaf_data({}, []), {})

    def test_popping_non_existent_path_pop_nothing(self):
        input_dict = {"a": 1}
        popped = gather_leaf_data(input_dict, [("b",)], pop=True)
        self.assertEqual(popped, {})
        self.assertEqual(input_dict, {"a": 1})

    def test_gathers_shallow_value(self):
        input_dict = {"a": 1}
        popped = gather_leaf_data(input_dict, [("a",)])
        self.assertEqual(popped, {"a": 1})
        self.assertEqual(input_dict, {"a": 1})

    def test_pops_shallow_value(self):
        input_dict = {"a": 1}
        popped = gather_leaf_data(input_dict, [("a",)], pop=True)
        self.assertEqual(popped, {"a": 1})
        self.assertEqual(input_dict, {})

    def test_gathers_leaf_of_deep_input_dict(self):
        input_dict = {"a": {"b": 1, "c": 2}}
        popped = gather_leaf_data(input_dict, [("a", "c")])
        self.assertEqual(popped, {"a": {"c": 2}})
        self.assertEqual(input_dict, {"a": {"b": 1, "c": 2}})

    def test_pops_leaf_of_deep_input_dict(self):
        input_dict = {"a": {"b": 1, "c": 2}}
        popped = gather_leaf_data(input_dict, [("a", "c")], pop=True)
        self.assertEqual(popped, {"a": {"c": 2}})
        self.assertEqual(input_dict, {"a": {"b": 1}})


class TestGetFileSize(unittest.TestCase):
    def test_get_file_size(self):
        s = 523
        expected_output = "523 B"
        output = get_file_size(s)
        self.assertEqual(expected_output, output)
        s = 2048
        expected_output = "2.0 KB"
        output = get_file_size(s)
        self.assertEqual(expected_output, output)
        s = 1024 * 1024 * 2
        expected_output = "2.0 MB"
        output = get_file_size(s)
        self.assertEqual(expected_output, output)
        s = 1024 * 1024 * 1024 * 2
        expected_output = "2.0 GB"
        output = get_file_size(s)
        self.assertEqual(expected_output, output)


class TestPythonInterpreter(unittest.TestCase):
    def test_resolve_python_interpreter(self):
        expected_path = "path_to_python"
        settings = TestAppSettings(expected_path)
        p = resolve_python_interpreter(settings)
        self.assertEqual(expected_path, p)
        settings = TestAppSettings("")
        p = resolve_python_interpreter(settings)
        self.assertEqual(sys.executable, p)
        if sys.platform == "win32":
            with mock.patch("spine_engine.utils.helpers.is_frozen") as mock_helpers_is_frozen:
                mock_helpers_is_frozen.return_value = True
                p = resolve_python_interpreter(settings)
                self.assertIsNone(p)  # This is None only on Win. # FIXME: Find a way to mock is_frozen() in config.py
                mock_helpers_is_frozen.assert_called()


class TestAppSettings:
    def __init__(self, test_path):
        self.test_path = test_path

    def value(self, key):
        if key == "appSettings/pythonPath":
            return self.test_path


if __name__ == "__main__":
    unittest.main()
