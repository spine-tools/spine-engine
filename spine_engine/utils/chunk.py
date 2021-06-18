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
Contains utilities needed for chunking DAGs.

:authors: A. Soininen (VTT)
:date:    21.6.2021
"""
from dataclasses import dataclass, field
import networkx as nx
from ..project_item.connection import Jump


@dataclass
class Chunk:
    item_names: set
    jump: Jump = field(default=None)


def chunkify(dag, jumps):
    """Breaks DAG into logical chunks.

    Args:
        dag (nx.DiGraph): directed acyclic graph
        jumps (list of Jump): jumps in DAG

    Returns:
        tuple of Chunk: chunks
    """
    if not dag:
        return tuple()
    if not jumps:
        return (Chunk(set(dag.nodes)),)
    jumps = _sort_jumps(jumps, dag)
    return _next_loop_chunk(dag, set(dag.nodes), jumps)


def _next_loop_chunk(dag, nodes, jumps):
    """Breaks consecutive jumps into chunks recursively.

    Args:
        dag (DiGraph): DAG
        nodes (set of str): remaining nodes
        jumps (list of Jump): remaining jumps
    """
    if not jumps:
        return (Chunk(nodes),)
    jump = jumps[0]
    source_nodes = tuple(node for node in dag.nodes() if dag.in_degree(node) == 0)
    initial_nodes = set()
    for source in source_nodes:
        for path in nx.all_simple_paths(dag, source, jump.source):
            initial_nodes.update(path)
        if source == jump.source:
            initial_nodes.add(source)
    chunks = list()
    if initial_nodes:
        chunks.append(Chunk(initial_nodes))
    loop_nodes = set(nx.shortest_path(nx.reverse_view(dag), jump.source, jump.destination))
    if loop_nodes:
        chunks.append(Chunk(loop_nodes, jump))
    remaining_nodes = set(nodes) - initial_nodes
    if remaining_nodes:
        dag.remove_nodes_from(initial_nodes)
        chunks += list(_next_loop_chunk(dag, remaining_nodes, jumps[1:]))
    return tuple(chunks)


def _sort_jumps(jumps, dag):
    """Sorts jumps by the rank of their source items.

    Args:
        jumps (list of Jump): jumps to sort
        dag (DiGraph): graph

    Returns:
        list of Jump: sorted jumps
    """
    ranked_items = list(nx.topological_sort(dag))
    return sorted(jumps, key=lambda j: ranked_items.index(j.source))
