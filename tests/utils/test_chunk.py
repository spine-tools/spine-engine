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
import networkx as nx
from spine_engine.project_item.connection import Jump
from spine_engine.utils.chunk import Chunk, chunkify


class TestChunkify(unittest.TestCase):
    def test_empty_dag(self):
        self.assertEqual(chunkify(nx.DiGraph(), []), tuple())

    def test_no_jump(self):
        dag = nx.DiGraph()
        dag.add_node("a")
        jumps = []
        expected = (Chunk({"a"}),)
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_single_looping_node(self):
        dag = nx.DiGraph()
        dag.add_node("a")
        jumps = [Jump("a", "top", "a", "bottom")]
        expected = (Chunk({"a"}, Jump("a", "top", "a", "bottom")),)
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_two_nodes_and_single_jump(self):
        dag = nx.DiGraph()
        dag.add_nodes_from(("a", "b"))
        dag.add_edge("a", "b")
        jumps = [Jump("b", "top", "a", "bottom")]
        expected = (Chunk({"a", "b"}, Jump("b", "top", "a", "bottom")),)
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_single_node_and_self_jump(self):
        dag = nx.DiGraph()
        dag.add_nodes_from(("a", "b"))
        dag.add_edge("a", "b")
        jumps = [Jump("b", "top", "b", "bottom")]
        expected = (Chunk({"a"})), Chunk({"b"}, Jump("b", "top", "b", "bottom"))
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_jump_in_diamond_branch(self):
        dag = nx.DiGraph()
        dag.add_nodes_from(("a", "b", "c", "d", "e"))
        dag.add_edges_from((("a", "b"), ("a", "c"), ("b", "d"), ("c", "e"), ("d", "e")))
        jumps = [Jump("d", "right", "b", "left")]
        expected = (Chunk({"a"}), Chunk({"b", "d"}, Jump("d", "right", "b", "left")), Chunk({"c", "e"}))
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_jump_after_diamond(self):
        dag = nx.DiGraph()
        dag.add_nodes_from(("a", "b", "c", "d", "e"))
        dag.add_edges_from((("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("d", "e")))
        jumps = [Jump("e", "bottom", "d", "top")]
        expected = (Chunk({"a", "b", "c"}), Chunk({"d", "e"}, Jump("e", "bottom", "d", "top")))
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_two_consecutive_jumps(self):
        dag = nx.DiGraph()
        dag.add_nodes_from(("a", "b"))
        dag.add_edge("a", "b")
        jumps = [Jump("a", "top", "a", "bottom"), Jump("b", "right", "b", "left")]
        expected = (Chunk({"a"}, Jump("a", "top", "a", "bottom")), Chunk({"b"}, Jump("b", "right", "b", "left")))
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_jump_sorting(self):
        dag = nx.DiGraph()
        dag.add_nodes_from(("a", "b"))
        dag.add_edge("a", "b")
        jumps = [Jump("b", "right", "b", "left"), Jump("a", "top", "a", "bottom")]
        expected = (Chunk({"a"}, Jump("a", "top", "a", "bottom")), Chunk({"b"}, Jump("b", "right", "b", "left")))
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_two_consecutive_jumps_with_padding_nodes(self):
        dag = nx.DiGraph()
        dag.add_nodes_from(("a", "b", "c", "d", "e", "f", "g"))
        dag.add_edges_from((("a", "b"), ("b", "c"), ("c", "d"), ("d", "e"), ("e", "f"), ("f", "g")))
        jumps = [Jump("c", "top", "b", "bottom"), Jump("f", "right", "e", "left")]
        expected = (
            Chunk({"a"}),
            Chunk({"b", "c"}, Jump("c", "top", "b", "bottom")),
            Chunk({"d"}),
            Chunk({"e", "f"}, Jump("f", "right", "e", "left")),
            Chunk({"g"}),
        )
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_jumps_in_parallel_branches(self):
        dag = nx.DiGraph()
        dag.add_nodes_from(("a", "b", "c", "d", "e"))
        dag.add_edges_from((("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("d", "e")))
        jumps = [Jump("d", "top", "c", "bottom"), Jump("b", "right", "b", "left")]
        expected = (
            Chunk({"a"}),
            Chunk({"b"}, Jump("b", "right", "b", "left")),
            Chunk({"d", "c"}, Jump("d", "top", "c", "bottom")),
            Chunk({"e"}),
        )
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_source_downstream_from_jump_source(self):
        dag = nx.DiGraph()
        dag.add_nodes_from(("a", "b", "c"))
        dag.add_edges_from((("a", "c"), ("b", "c")))
        jumps = [Jump("a", "top", "a", "bottom")]
        expected = (Chunk({"a"}, Jump("a", "top", "a", "bottom")), Chunk({"b", "c"}))
        self.assertEqual(chunkify(dag, jumps), expected)

    def test_nested_jumps(self):
        dag = nx.DiGraph()
        dag.add_nodes_from(("a", "b", "c"))
        dag.add_edges_from((("a", "b"), ("b", "c")))
        jumps = [Jump("c", "top", "a", "bottom"), Jump("b", "right", "b", "left")]
        expected = (
            Chunk({"a"}, None),
            Chunk({"b"}, Jump("b", "right", "b", "left")),
            Chunk({"c"}, Jump("c", "top", "a", "bottom")),
        )
        self.assertEqual(chunkify(dag, jumps), expected)


if __name__ == '__main__':
    unittest.main()
