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
    item_names = set(dag.nodes)
    if not jumps:
        return (Chunk(item_names),)
    end_points = _sorted_jump_end_points(jumps, dag)
    jumps_by_source = {jump.source: jump for jump in jumps}
    return _cut_chunks(dag, item_names, end_points, jumps_by_source)


def _sorted_jump_end_points(jumps, dag):
    """Sorts jump source and destination item names by their rank in DAG.

    Args:
        jumps (list of Jump): jumps to sort
        dag (DiGraph): graph

    Returns:
        list of Jump: sorted jumps
    """
    ranked_item_names = {name: i for i, name in enumerate(list(nx.topological_sort(dag)))}
    sources = [jump.source for jump in jumps]
    end_points = [jump.destination for jump in jumps] + sources
    sorted_points = sorted(end_points, key=lambda p: ranked_item_names[p])
    duplicates_merged = list()
    for point, next_point in zip(sorted_points[:-1], sorted_points[1:]):
        if point != next_point or point in sources:
            duplicates_merged.append(point)
    duplicates_merged.append(sorted_points[-1])
    return duplicates_merged


def _cut_chunks(dag, unchunked_items, jump_end_points, jumps):
    """Cuts dag into chunks recursively.

    Args:
        dag (DiGraph): DAG to chunk
        unchunked_items (set of str): names of items that haven't been chunked yet
        jump_end_points (Sequence of str): jump end points
        jumps (dict): mapping from jump source to Jump

    Returns:
        tuple of Chunk: chunks
    """
    if not jump_end_points:
        return (Chunk(unchunked_items),)
    end_point = jump_end_points[0]
    source_nodes = tuple(node for node in dag.nodes() if dag.in_degree(node) == 0)
    next_jump_is_self_jump = len(jump_end_points) > 1 and end_point == jump_end_points[1]
    if source_nodes == {end_point} and next_jump_is_self_jump:
        chunks = [Chunk({end_point}, jumps[end_point])]
        remaining_items = unchunked_items - {end_point}
        if remaining_items:
            chunks += list(_cut_chunks(dag.subgraph(remaining_items), remaining_items, jump_end_points[2:], jumps))
        return tuple(chunks)
    chunk_items = set()
    for source in source_nodes:
        for path in nx.all_simple_paths(dag, source, end_point):
            chunk_items.update(path)
        if source == end_point:
            chunk_items.add(source)
        if end_point not in jumps or next_jump_is_self_jump:
            chunk_items.discard(end_point)
    chunks = list()
    if chunk_items:
        jump = jumps.get(end_point) if end_point in chunk_items else None
        chunks.append(Chunk(chunk_items, jump))
    remaining_items = set(unchunked_items) - chunk_items
    if remaining_items:
        chunks += list(_cut_chunks(dag.subgraph(remaining_items), remaining_items, jump_end_points[1:], jumps))
    return tuple(chunks)
