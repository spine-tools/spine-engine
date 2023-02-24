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
Unit tests for chunk module.

:authors: A. Soininen (VTT)
:date:    21.6.2021
"""
import unittest
from spinedb_api.helpers import remove_credentials_from_url
from spine_engine.utils.helpers import make_dag, gather_leaf_data, get_file_size


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


if __name__ == '__main__':
    unittest.main()
