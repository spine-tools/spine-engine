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
from spine_engine.utils.helpers import make_dag


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


if __name__ == '__main__':
    unittest.main()
