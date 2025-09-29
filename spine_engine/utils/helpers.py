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

"""Helper functions and classes."""
from __future__ import annotations
import collections
from collections.abc import Hashable, Iterable
import datetime
from enum import Enum, auto, unique
import itertools
import json
import os
import pathlib
import sys
import time
from typing import TYPE_CHECKING, Type
from jupyter_client.kernelspec import KernelSpecManager
import networkx
import networkx as nx
from spinedb_api.spine_io.gdx_utils import find_gams_directory
from ..config import EMBEDDED_PYTHON, GAMS_EXECUTABLE, JULIA_EXECUTABLE, PYTHON_EXECUTABLE, is_frozen

if TYPE_CHECKING:
    from ..project_item.connection import Connection
    from ..project_item.executable_item_base import ExecutableItemBase


@unique
class ExecutionDirection(Enum):
    FORWARD = auto()
    BACKWARD = auto()
    NONE = auto()

    def __str__(self):
        return str(self.name)


@unique
class ItemExecutionFinishState(Enum):
    SUCCESS = 1
    FAILURE = 2
    SKIPPED = 3
    EXCLUDED = 4
    STOPPED = 5
    NEVER_FINISHED = 6

    def __str__(self):
        return str(self.name)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class AppSettings:
    """A QSettings replacement."""

    def __init__(self, settings: dict[str, str]):
        self._settings = settings

    def value(self, key: str, defaultValue: str = "") -> str:
        return self._settings.get(key, defaultValue)


def shorten(name: str) -> str:
    """Returns the 'short name' version of given name."""
    return name.lower().replace(" ", "_")


def create_log_file_timestamp() -> str:
    """Creates a new timestamp string that is used as Data Store and Importer error log file.

    Returns:
        Timestamp string or empty string if failed.
    """
    try:
        # Create timestamp
        stamp = datetime.datetime.fromtimestamp(time.time())
    except OverflowError:
        return ""
    extension = stamp.strftime("%Y%m%dT%H%M%S")
    return extension


def create_timestamp() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def resolve_conda_executable(conda_path: str) -> str:
    """If given conda_path is an empty str, returns current Conda
    executable from CONDA_EXE env variable if the app was started
    on Conda, otherwise returns an empty string.
    """
    if conda_path != "":
        return conda_path
    conda_exe = os.environ.get("CONDA_EXE", "")
    return conda_exe


def resolve_python_interpreter(settings: AppSettings) -> str:
    """Returns a path to Python interpreter in settings or the current executable if none is set.

    Args:
        settings: settings

    Returns:
        path to Python interpreter
    """
    path = settings.value("appSettings/pythonPath")
    if path:
        return path
    return resolve_current_python_interpreter()


def resolve_current_python_interpreter() -> str:
    """Returns a path to current Python interpreter.

    Returns:
        path to Python interpreter
    """
    if not is_frozen():
        return sys.executable
    if not sys.platform == "win32":
        path = resolve_executable_from_path(PYTHON_EXECUTABLE)
        if path != "":
            return path
    return EMBEDDED_PYTHON


def resolve_julia_executable(settings: AppSettings) -> str:
    """Returns path to Julia executable from settings, and, if not set, path to default Julia executable.

    Args:
        settings: application settings

    Returns:
        path to Julia executable
    """
    path = settings.value("appSettings/juliaPath")
    if path:
        return path
    return resolve_default_julia_executable()


def resolve_default_julia_executable() -> str:
    """Returns path to default Julia executable.

    Tries to find the path to Julia in user's PATH env variable.
    If Julia is not found in PATH, returns an empty string.

    Returns:
        path to Julia executable
    """
    return resolve_executable_from_path(JULIA_EXECUTABLE)


def resolve_julia_project(settings: AppSettings) -> str:
    """Returns path to Julia environment (project) from settings or an empty string if not available.

    Args:
        settings: application settings

    Returns:
        path to Julia environment
    """
    return settings.value("appSettings/juliaProjectPath")


def resolve_gams_executable(gams_path: str) -> str:
    """If given gams_path is empty, tries to find the path to GAMS executable.

    If GAMS is not found, returns an empty string.

    Args:
        gams_path: current path to GAMS executable

    Returns:
        resolved path to GAMS executable
    """
    if gams_path != "":
        return gams_path
    gams_dir = find_gams_directory()
    if gams_dir is None:
        return ""
    return os.path.join(gams_dir, GAMS_EXECUTABLE)


def resolve_executable_from_path(executable_name: str) -> str:
    """Returns full path to executable name in user's
    PATH env variable. If not found, returns an empty string.

    Basically equivalent to 'where' and 'which' commands in
    cmd.exe and bash respectively.

    Args:
        executable_name: Executable filename to find (e.g. python.exe, julia.exe)

    Returns:
        Full path or empty string
    """
    executable_paths = os.get_exec_path()
    for path in executable_paths:
        candidate = os.path.join(path, executable_name)
        if os.path.isfile(candidate):
            return candidate
    return ""


def custom_find_kernel_specs(ensure_native_kernel: bool = True) -> dict[str, str]:
    """Finds kernel specs including the native kernel if enabled.

    Args:
        ensure_native_kernel: True includes the native kernel (python3 for Python) into the
        returned dict, False skips it.

    Returns:
        A dict mapping kernel names to resource directories
    """
    ksm = KernelSpecManager()
    ksm.ensure_native_kernel = ensure_native_kernel
    return ksm.find_kernel_specs()


def inverted(input_: dict[Hashable, Iterable[Hashable]]) -> dict[Hashable, list[Hashable]]:
    """Inverts a dictionary of list values.

    Args:
        input_: dictionary to invert

    Returns:
        keys are list items, and values are keys listing that item from the input dictionary
    """
    output = {}
    for key, value_list in input_.items():
        for value in value_list:
            output.setdefault(value, list()).append(key)
    return output


def get_julia_env(
    use_jupyter_console: bool, julia_kernel: str, julia_path: str, julia_project_path: str
) -> tuple[str, str] | None:
    """
    Args:
        use_jupyter_console: True if Jupyter console is in use
        julia_kernel: Julia kernel name
        julia_path: Path to Julia executable
        julia_project_path: Path to Julia project/environment folder

    Returns:
        (julia_exe, julia_project), or None if none found
    """
    if use_jupyter_console:
        resource_dir = custom_find_kernel_specs().get(julia_kernel)
        if resource_dir is None:
            return None
        filepath = os.path.join(resource_dir, "kernel.json")
        with open(filepath, "r") as fh:
            try:
                kernel_spec = json.load(fh)
            except json.decoder.JSONDecodeError:
                return None
        julia = kernel_spec["argv"].pop(0)
        project_arg = next((arg for arg in kernel_spec["argv"] if arg.startswith("--project=")), None)
        project = "" if project_arg is None else project_arg.split("--project=")[1]
        return julia, project
    if julia_path == "":
        julia_path = resolve_executable_from_path(JULIA_EXECUTABLE)
        if julia_path == "":
            return None
    return julia_path, julia_project_path


def required_items_for_execution(
    items: dict[str, dict],
    connections: list[Connection],
    executable_item_classes: dict[str, Type[ExecutableItemBase]],
    execution_permits: dict[str, bool],
) -> set[str]:
    """Builds a list of names of items that are required for execution.

    An item is required if

    - it has an execution permit
    - the item is part of a filtered fork that contains an item that has an execution permit

    Args:
        items: mapping from item name to item dict
        connections: connections
        executable_item_classes: mapping from item type to its executable class
        execution_permits: item execution permits

    Returns:
        names of required items
    """
    first_filter_fork_nodes = _first_filter_fork_nodes(connections)
    filter_fork_terminus_nodes = _filter_fork_termini(items, executable_item_classes)
    dependent_paths = _filtered_fork_paths(
        make_dag(dag_edges(connections), execution_permits), first_filter_fork_nodes, filter_fork_terminus_nodes
    )
    items_required_predecessors = _dependent_items_per_item(dependent_paths)
    required_items = {item for item, is_permitted in execution_permits.items() if is_permitted}
    for item in list(required_items):
        required_items |= items_required_predecessors.get(item, set())
    return required_items


def _first_filter_fork_nodes(connections: Iterable[Connection]) -> set[str]:
    """Collects nodes that start a filtered fork.

    Args:
        connections: connections

    Returns:
        item names
    """
    nodes = set()
    for connection in connections:
        if connection.has_filters_online():
            nodes.add(connection.destination)
    return nodes


def _filter_fork_termini(
    items: dict[str, dict], executable_item_classes: dict[str, Type[ExecutableItemBase]]
) -> set[str]:
    """Collects nodes that terminate a filtered fork.

    Args:
        items: mapping from item name to item dict
        executable_item_classes: mapping from item type to corresponding executable item class

    Returns:
        set of str: item names
    """
    termini = set()
    for name, item_dict in items.items():
        if executable_item_classes[item_dict["type"]].is_filter_terminus():
            termini.add(name)
    return termini


def _filtered_fork_paths(
    dag: nx.DiGraph, first_filter_fork_nodes: set[str], filter_fork_terminus_nodes: set[str]
) -> list[list[str]]:
    """Collects all simple paths within given DAG that will be forked.

    Args:
        dag: DAG
        first_filter_fork_nodes: names of fork staring items
        filter_fork_terminus_nodes: names of fork ending items

    Returns:
        items names along the paths
    """
    sources = [node for node, in_degree in dag.in_degree if in_degree == 0]
    targets = [node for node, out_degree in dag.out_degree if out_degree == 0]
    paths = []
    for source, target in itertools.product(sources, targets):
        for path in networkx.all_simple_paths(dag, source, target):
            gather = False
            gathered = []
            for node in path:
                if node in first_filter_fork_nodes:
                    gather = True
                if node in filter_fork_terminus_nodes and gather:
                    if gathered:
                        paths.append(gathered)
                        gathered = []
                    gather = False
                if gather:
                    gathered.append(node)
            if gathered:
                paths.append(gathered)
    return paths


def _dependent_items_per_item(fork_paths: list[list[str]]) -> dict[str, set[str]]:
    """Collects dependent items for each project item.

    Args:
        fork_paths: item names along fork paths

    Returns:
        mapping from item name to a set of the names of its dependant items
    """
    items_dependent_nodes = collections.defaultdict(set)
    for path in fork_paths:
        for i, node in enumerate(path[1:]):
            items_dependent_nodes[node] |= set(path[: i + 1])
    return items_dependent_nodes


def make_connections(connections: list[Connection], permitted_items: set[str]) -> list[Connection]:
    """Returns a list of Connections based on permitted
    items. Creates Connections only for connections that
    are coming from permitted items or leaving from
    permitted items.

    Args:
        connections: connections in the DAG
        permitted_items: names of permitted items

    Returns:
        List of permitted Connections or an empty list if the DAG contains no connections
    """
    if not connections:
        return []
    connections = connections_to_selected_items(connections, permitted_items)
    return connections


def connections_to_selected_items(connections: list[Connection], selected_items: set[str]) -> list[Connection]:
    """Returns a list of Connections that have a permitted item
    as its source or destination item.

    Args:
        connections: List of Connections
        selected_items: names of permitted items

    Returns:
        Connections allowed in the current DAG
    """
    return [conn for conn in connections if conn.source in selected_items or conn.destination in selected_items]


def dag_edges(connections: list[Connection]) -> dict[str, list[str]]:
    """Collects DAG edges based on Connection instances.

    Args:
        connections: Connections

    Returns:
        DAG edges. Mapping of source item (node) to a list of destination items (nodes)
    """
    edges = {}
    for connection in connections:
        source, destination = connection.source, connection.destination
        edges.setdefault(source, []).append(destination)
    return edges


def make_dag(edges: dict[str, list[str]], permitted_nodes: dict[str, bool] | None = None) -> nx.DiGraph:
    """Builds a DAG from edges or if no edges exist, from permitted_nodes.

    Args:
        edges: Mapping from item name to list of its successors' names
        permitted_nodes: Mapping from item name to boolean value indicating if item is selected

    Returns:
        Directed acyclic graph
    """
    graph = networkx.DiGraph()
    if not edges:
        # Make a single node DAG with no edges
        nodes = [node_name for node_name, permitted in permitted_nodes.items() if permitted]
        graph.add_nodes_from(nodes)
    else:
        graph.add_nodes_from(edges)
        for node, successors in edges.items():
            if successors is None:
                continue
            for successor in successors:
                graph.add_edge(node, successor)
    return graph


def write_filter_id_file(filter_id: str, path: pathlib.Path | str) -> None:
    """Writes filter id to disk.

    Args:
        filter_id : filter id
        path: full path to directory where the filter id file will be written
    """
    with pathlib.Path(path, ".filter_id").open("w") as filter_id_file:
        filter_id_file.writelines([filter_id + "\n"])


def gather_leaf_data(input_dict: dict, paths: list[tuple], pop: bool = False) -> dict:
    """Gathers data defined by 'paths' of keys from nested dicts.

    Args:
        input_dict: dict to pop from
        paths: 'paths' of dict keys to leaf entries
        pop: if True, pops the leaf data modifying ''input_dict''

    Returns:
        popped data
    """

    def travel_to_leaf(dict_to_travel, path_to_leaf):
        traveller = dict_to_travel
        for part in path_to_leaf[:-1]:
            traveller = traveller.get(part)
            if traveller is None:
                return None
        return traveller

    def build_to_leaf(base_dict, path_to_leaf):
        builder = base_dict
        for part in path_to_leaf[:-1]:
            builder = builder.setdefault(part, {})
        return builder

    gather = "pop" if pop else "get"
    output_dict = {}
    for prefix in paths:
        leaf_dict = travel_to_leaf(input_dict, prefix)
        if leaf_dict is None:
            continue
        value = getattr(leaf_dict, gather)(prefix[-1], None)
        if value is None:
            continue
        leaf_dict = build_to_leaf(output_dict, prefix)
        leaf_dict[prefix[-1]] = value
    return output_dict


def get_file_size(size_in_bytes: int) -> str:
    """Returns a human readable string of the size of a file. Given size_in_bytes arg
    is designed as the output of os.path.getsize().

    1 KB = 1024 bytes
    1 MB = 1024*1024 bytes
    1 GB = 1024*1024*1024 bytes

    Args:
        size_in_bytes: Size in bytes [B]

    Returns:
        Human readable file size
    """
    kb = 1024
    mb = 1024 * 1024
    gb = 1024 * 1024 * 1024
    if size_in_bytes <= kb:
        return str(size_in_bytes) + " B"
    if kb < size_in_bytes <= mb:
        return str(round(size_in_bytes / kb, 1)) + " KB"
    elif mb < size_in_bytes < gb:
        return str(round(size_in_bytes / mb, 1)) + " MB"
    else:
        return str(round(size_in_bytes / gb, 1)) + " GB"


class PartCount:
    def __init__(self):
        self._count = 0

    def __iadd__(self, number):
        self._count += number
        return self

    def __eq__(self, number):
        return self._count == number

    def __deepcopy__(self, memo):
        return self

    def __repr__(self):
        return str(self._count)


def serializable_error_info_from_exc_info(exc_info):
    return exc_info


def urls_equal(url1: str, url2: str) -> bool:
    if url1.startswith("sqlite://"):
        if not url2.startswith("sqlite://"):
            return False
        return _file_urls_equal(url1, url2, len("sqlite:///"))
    if url1.startswith("file://"):
        if not url2.startswith("file://"):
            return False
        return _file_urls_equal(url1, url2, len("file://"))
    return url1 == url2


def _file_urls_equal(url1: str, url2: str, prefix_length: int) -> bool:
    return pathlib.Path(url1[prefix_length:]) == pathlib.Path(url2[prefix_length:])
