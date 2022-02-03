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
    items: list
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
        return ()
    if not jumps:
        return (Chunk(list(dag.nodes)),)
    order = {name: i for i, name in enumerate(list(nx.topological_sort(dag)))}
    # Collect loop chunks
    items_by_jump = _items_by_jump(dag, jumps)
    nested_jumps = _nested_jumps(jumps, items_by_jump)
    top_level_jumps = {jump for jump in jumps if not any(jump in nested for nested in nested_jumps.values())}
    loop_chunks = [Chunk(list(items_by_jump[jump]), jump) for jump in top_level_jumps]
    # Collect chunks in between loops, by moving from sources to successive 'bands' of parallel loops, to sinks
    _sort(dag, loop_chunks, order)
    groups = _group_parallel(dag, loop_chunks)
    non_loop_chunks = []
    loop_items = set(item for items in items_by_jump.values() for item in items)
    sources = set(node for node in dag.nodes() if dag.in_degree(node) == 0)
    for group in groups:
        sinks = set(item for chunk in group for item in chunk.items)
        connecting_items = _connecting_items(dag, sources, sinks) - loop_items
        if connecting_items:
            non_loop_chunks.append(Chunk(list(connecting_items)))
        sources = sinks
    sinks = set(node for node in dag.nodes() if dag.out_degree(node) == 0)
    connecting_items = _connecting_items(dag, sources, sinks) - loop_items
    if connecting_items:
        non_loop_chunks.append(Chunk(list(connecting_items)))
    # Finally collect chunks from all remaining items
    chunks = loop_chunks + non_loop_chunks
    remaining_items = set(dag.nodes) - set(item for chunk in chunks for item in chunk.items)
    for items in nx.connected_components(dag.subgraph(remaining_items).to_undirected()):
        chunk = Chunk(sorted(items, key=lambda p: order[p]))
        chunk_count = len(chunks)
        left = next(
            (k for k in reversed(range(chunk_count)) if order[chunks[k].items[-1]] < order[chunk.items[0]]),
            chunk_count,
        )
        chunk_left = chunks[left]
        chunk_right = chunks[left + 1] if left + 1 < chunk_count else None
        if chunk_left.jump is None:
            chunk_left.items += chunk.items
        elif chunk_right is not None and chunk_right.jump is None:
            chunk_right.items += chunk.items
        else:
            chunks.insert(left, chunk)
    # Split nested loops
    _sort(dag, chunks, order)
    _split_nested(dag, chunks, nested_jumps)
    return tuple(chunks)


def _sort(dag, chunks, order):
    """Sorts items within chunks and chunks within the list by given order.

    Args:
        dag (nx.DiGraph): directed acyclic graph
        chunks (list of Chunk): chunks in DAG
    """
    for chunk in chunks:
        chunk.items.sort(key=lambda p: order[p])
    chunks.sort(key=lambda chunk: order[chunk.items[0]])


def _items_by_jump(dag, jumps):
    """Finds items by jump.

    Args:
        dag (nx.DiGraph): directed acyclic graph
        jumps (list of Jump): jumps in DAG

    Returns:
        dict: mapping jumps to internal items
    """
    items_by_jump = {}
    for jump in jumps:
        items = items_by_jump[jump] = {jump.destination, jump.source}
        for path in nx.all_simple_paths(dag, jump.destination, jump.source):
            items.update(path)
    return items_by_jump


def _nested_jumps(jumps, items_by_jump):
    """Finds nested jumps.

    Args:
        jumps (list of Jump): jumps in DAG
        items_by_jump (dict): mapping jumps to internal items

    Returns:
        dict: mapping jumps to a set of nested jumps
    """
    nested_jumps = {}
    sorted_jumps = sorted(jumps, key=lambda jump: len(items_by_jump[jump]))
    for i, jump in enumerate(sorted_jumps):
        for other_jump in sorted_jumps[i + 1 :]:
            other_items = items_by_jump[other_jump]
            if jump.source in other_items and jump.destination in other_items:
                nested_jumps.setdefault(other_jump, set()).add(jump)
                break
    return nested_jumps


def _group_parallel(dag, chunks):
    """Groups parallel chunks.

    Args:
        dag (nx.DiGraph): directed acyclic graph
        chunks (list of Chunk)

    Returns:
        list of list: Each element in the outer list is a list of chunks that don't reach each other.
    """
    parallel_indexes = dict()
    already_grouped = set()
    for i, chunk in enumerate(chunks):
        if i in already_grouped:
            continue
        parallel_indexes[i] = parallel = list()
        for j in range(i + 1, len(chunks)):
            if not any(nx.has_path(dag, a, b) or nx.has_path(dag, b, a) for a in chunk.items for b in chunks[j].items):
                parallel.append(j)
                already_grouped.add(j)
    return [[chunks[i]] + [chunks[j] for j in parallel] for i, parallel in parallel_indexes.items()]


def _connecting_items(dag, sources, sinks):
    """Returns all items connecting sources and sinks in a dag.

    Args:
        dag (nx.DiGraph): directed acyclic graph
        sources (Iterable of str)
        sinks (Iterable of str)

    Returns:
        set of str
    """
    return {
        item for source in sources for sink in sinks for path in nx.all_simple_paths(dag, source, sink) for item in path
    }


def _split_nested(dag, chunks, nested_jumps):
    """Splits chunks that contain nested jumps.

    Args:
        dag (nx.DiGraph): directed acyclic graph
        chunks (list of Chunk): chunks in DAG
        nested_jumps (dict): mapping jumps to a set of nested jumps
    """
    while nested_jumps:
        for i in reversed(range(len(chunks))):
            chunk = chunks[i]
            items = chunk.items
            nested = nested_jumps.pop(chunk.jump, None)
            if nested is None:
                continue
            intervals = {j: (items.index(j.destination), items.index(j.source)) for j in nested}
            intervals = dict(sorted(intervals.items(), key=lambda x: x[1]))
            inner_chunks = []
            tip = 0
            for jump, (start, end) in intervals.items():
                if tip < start:
                    inner_chunks += [Chunk(items[tip:start])]
                tip = end + 1
                inner_chunks += [Chunk(items[start:tip], jump)]
            inner_chunks += [Chunk(items[tip:], chunk.jump)]
            chunks.pop(i)
            chunks[i:i] = inner_chunks
