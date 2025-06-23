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
from collections.abc import Iterator
import networkx as nx
import networkx.exception as nx_exception
from .backward_jump import BackwardJump
from .connection import Connection
from .exception import ProjectException
from .project_item import ProjectItem
from .project_item_specification import ProjectItemSpecification


class Project:
    def __init__(self, name: str, description: str = ""):
        self._graph: nx.DiGraph[str] = nx.DiGraph(name=name, description=description, specifications={})

    def name(self) -> str:
        return self._graph.graph["name"]

    def description(self) -> str:
        return self._graph.graph["description"]

    def item_name_iter(self) -> Iterator[str]:
        yield from self._graph.nodes

    def add_item(self, name: str, item: ProjectItem) -> None:
        if self._graph.has_node(name):
            raise ProjectException(f"project already contains {name}")
        self._graph.add_node(name, item=item)

    def item(self, name: str) -> ProjectItem:
        try:
            node = self._graph.nodes[name]
        except KeyError:
            raise ProjectException("no such item")
        return node["item"]

    def rename_item(self, name: str, new_name: str) -> None:
        try:
            node = self._graph.nodes[name]
        except KeyError:
            raise ProjectException("no such item")
        if self._graph.has_node(new_name):
            raise ProjectException(f"project already contains {new_name}")
        item = node["item"]
        in_connections = tuple((edge[0], self._graph.edges[edge]["connection"]) for edge in self._graph.in_edges(name))
        out_connections = tuple(
            (edge[1], self._graph.edges[edge]["connection"]) for edge in self._graph.out_edges(name)
        )
        self._graph.remove_node(name)
        self._graph.add_node(new_name, item=item)
        for predecessor_name, connection in in_connections:
            self._graph.add_edge(predecessor_name, new_name, connection=connection)
        for successor_name, connection in out_connections:
            self._graph.add_edge(new_name, successor_name, connection=connection)

    def remove_item(self, name: str) -> None:
        try:
            self._graph.remove_node(name)
        except nx_exception.NetworkXError as error:
            raise ProjectException("no such item") from error

    def add_connection(self, source: str, target: str, connection: Connection) -> None:
        if source not in self._graph.nodes:
            raise ProjectException("no such source item")
        if target not in self._graph.nodes:
            raise ProjectException("no such target item")
        if source == target:
            raise ProjectException(f"cannot connect {source} to itself")
        if self._graph.has_edge(source, target):
            raise ProjectException(f"{source} and {target} are already connected")
        self._graph.add_edge(source, target, connection=connection)
        if not self._is_dag():
            self._graph.remove_edge(source, target)
            raise ProjectException(f"connecting {source} to {target} would create a cycle")

    def in_connection_iter(self, target: str) -> Iterator[tuple[str, Connection]]:
        for edge in self._graph.in_edges(target):
            yield edge[0], self._graph.edges[edge]["connection"]

    def out_connection_iter(self, source: str) -> Iterator[tuple[str, Connection]]:
        for edge in self._graph.out_edges(source):
            yield edge[1], self._graph.edges[edge]["connection"]

    def connection(self, source: str, target: str) -> Connection:
        try:
            return self._graph.edges[source, target]["connection"]
        except KeyError as error:
            raise ProjectException("no such connection") from error

    def remove_connection(self, source: str, target: str) -> None:
        try:
            self._graph.remove_edge(source, target)
        except nx_exception.NetworkXError as error:
            raise ProjectException("no such connection") from error

    def add_backward_jump(self, source: str, target: str, jump: BackwardJump) -> None:
        if source not in self._graph.nodes:
            raise ProjectException("no such source item")
        if target not in self._graph.nodes:
            raise ProjectException("no such target item")
        self._graph.add_edge(source, target, backward_jump=jump)

    def backward_jump(self, source: str, target: str) -> BackwardJump:
        try:
            return self._graph.edges[source, target]["backward_jump"]
        except KeyError as error:
            raise ProjectException("no such jump") from error

    def remove_backward_jump(self, source: str, target: str) -> None:
        try:
            self._graph.remove_edge(source, target)
        except nx_exception.NetworkXError as error:
            raise ProjectException("no such jump") from error

    def _is_dag(self) -> bool:
        return nx.is_directed_acyclic_graph(self._connections_only())

    def _connections_only(self) -> nx.DiGraph:
        def accept_connections_only(source: str, target: str) -> bool:
            return "connection" in self._graph.edges[source, target]

        return nx.subgraph_view(self._graph, filter_edge=accept_connections_only)

    def add_item_specification(self, specification: ProjectItemSpecification) -> None:
        specification_list: list[ProjectItemSpecification] = self._graph.graph["specifications"].setdefault(
            specification.item_type, []
        )
        for existing in specification_list:
            if specification.name == existing.name:
                raise ProjectException(f"project already contains {specification.name}")
        specification_list.append(specification)

    def item_specification_iter(self, item_type: str) -> Iterator[ProjectItemSpecification]:
        yield from self._graph.graph["specifications"][item_type]

    def item_specification(self, item_type: str, name: str) -> ProjectItemSpecification:
        specification_list: list[ProjectItemSpecification] = self._graph.graph["specifications"][item_type]
        for specification in specification_list:
            if specification.name == name:
                return specification
        raise ProjectException("no such specification")

    def remove_item_specification(self, item_type: str, name: str) -> None:
        specification_list: list[ProjectItemSpecification] = self._graph.graph["specifications"][item_type]
        for i, specification in enumerate(specification_list):
            if specification.name == name:
                del specification_list[i]
                return
        raise ProjectException("no such specification")
