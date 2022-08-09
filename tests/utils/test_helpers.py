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
Unit tests for chunk module.

:authors: A. Soininen (VTT)
:date:    21.6.2021
"""
import unittest
from spine_engine.utils.helpers import make_dag, remove_credentials_from_url, gather_leaf_data


class TestMakeDAG(unittest.TestCase):
    def test_empty_dag(self):
        node_successors = {}
        dag = make_dag(node_successors)
        self.assertFalse(dag)

    def test_single_node(self):
        node_successors = {"a": None}
        dag = make_dag(node_successors)
        self.assertEqual(len(dag), 1)

    def test_two_nodes(self):
        node_successors = {"a": ["b"], "b": None}
        dag = make_dag(node_successors)
        self.assertEqual(set(dag.nodes), {"a", "b"})
        self.assertEqual(list(dag.edges), [("a", "b")])

    def test_branch(self):
        node_successors = {"a": ["b", "c"], "b": None, "c": None}
        dag = make_dag(node_successors)
        self.assertEqual(set(dag.nodes), {"a", "b", "c"})
        self.assertEqual(set(dag.edges), {("a", "b"), ("a", "c")})


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


if __name__ == '__main__':
    unittest.main()
