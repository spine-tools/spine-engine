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
Helpers functions and classes.

:authors: M. Marin (KTH)
:date:   20.11.2019
"""
import os
import sys
import datetime
import time
import json
from pathlib import Path
from enum import Enum, auto, unique
import networkx
from jupyter_client.kernelspec import find_kernel_specs

from spinedb_api.spine_io.gdx_utils import find_gams_directory
from ..config import PYTHON_EXECUTABLE, JULIA_EXECUTABLE, GAMS_EXECUTABLE, EMBEDDED_PYTHON


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
    """
    A QSettings replacement.
    """

    def __init__(self, settings):
        """
        Init.

        Args:
            settings (dict)
        """
        self._settings = settings

    def value(self, key, defaultValue=""):
        return self._settings.get(key, defaultValue)


def shorten(name):
    """Returns the 'short name' version of given name."""
    return name.lower().replace(" ", "_")


def create_log_file_timestamp():
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


def create_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def resolve_conda_executable(conda_path):
    """If given conda_path is an empty str, returns current Conda
    executable from CONDA_EXE env variable if the app was started
    on Conda, otherwise returns an empty string.
    """
    if conda_path != "":
        return conda_path
    conda_exe = os.environ.get("CONDA_EXE", "")
    return conda_exe


def resolve_python_interpreter(python_path):
    """If given python_path is empty, returns the
    full path to Python interpreter depending on user's
    settings and whether the app is frozen or not.
    """
    if python_path != "":
        return python_path
    if not getattr(sys, "frozen", False):
        return sys.executable  # Use current Python
    # We are frozen
    path = resolve_executable_from_path(PYTHON_EXECUTABLE)
    if path != "":
        return path  # Use Python from PATH
    return EMBEDDED_PYTHON  # Use embedded <app_install_dir>/Tools/python.exe


def resolve_julia_executable(julia_path):
    """if given julia_path is empty, tries to find the path to Julia
    in user's PATH env variable. If Julia is not found in PATH,
    returns an empty string.

    Note: In the long run, we should decide whether this is something we want to do
    because adding julia-x.x./bin/ dir to the PATH is not recommended because this
    also exposes some .dlls to other programs on user's (windows) system. I.e. it
    may break other programs, and this is why the Julia installer does not
    add (and does not even offer the chance to add) Julia to PATH.
    """
    if julia_path != "":
        return julia_path
    return resolve_executable_from_path(JULIA_EXECUTABLE)


def resolve_gams_executable(gams_path):
    """If given gams_path is empty, tries to find the path to GAMS executable.

    If GAMS is not found, returns an empty string.

    Args:
        gams_path (str): current path to GAMS executable

    Returns:
        str: resolved path to GAMS executable
    """
    if gams_path != "":
        return gams_path
    gams_dir = find_gams_directory()
    if gams_dir is None:
        return ""
    return os.path.join(gams_dir, GAMS_EXECUTABLE)


def resolve_executable_from_path(executable_name):
    """Returns full path to executable name in user's
    PATH env variable. If not found, returns an empty string.

    Basically equivalent to 'where' and 'which' commands in
    cmd.exe and bash respectively.

    Args:
        executable_name (str): Executable filename to find (e.g. python.exe, julia.exe)

    Returns:
        str: Full path or empty string
    """
    executable_paths = os.get_exec_path()
    for path in executable_paths:
        candidate = os.path.join(path, executable_name)
        if os.path.isfile(candidate):
            return candidate
    return ""


def inverted(input_):
    """Inverts a dictionary of list values.

    Args:
        input_ (dict)

    Returns:
        dict: keys are list items, and values are keys listing that item from the input dictionary
    """
    output = dict()
    for key, value_list in input_.items():
        for value in value_list:
            output.setdefault(value, list()).append(key)
    return output


def get_julia_command(settings):
    """
    Args:
        settings (QSettings, AppSettings)

    Returns:
        list: e.g. ["path/to/julia", "--project=path/to/project/"]
    """
    env = get_julia_env(settings)
    if env is None:
        return None
    julia, project = env
    return [julia, f"--project={project}"]


def get_julia_env(settings):
    """
    Args:
        settings (QSettings, AppSettings)

    Returns:
        tuple, NoneType: (julia_exe, julia_project), or None if none found
    """
    use_julia_kernel = settings.value("appSettings/useJuliaKernel", defaultValue="2") == "2"
    if use_julia_kernel:
        kernel_name = settings.value("appSettings/juliaKernel", defaultValue="")
        resource_dir = find_kernel_specs().get(kernel_name)
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
    julia = settings.value("appSettings/juliaPath", defaultValue="")
    if julia == "":
        julia = resolve_executable_from_path(JULIA_EXECUTABLE)
        if julia == "":
            return None
    project = settings.value("appSettings/juliaProjectPath", defaultValue="")
    return julia, project


def make_dag(node_successors):
    """Builds a DAG from node successors.

    Args:
        node_successors (dict): mapping from item name to list of its successors' names

    Returns:
        DiGraph: directed acyclic graph
    """
    graph = networkx.DiGraph()
    graph.add_nodes_from(node_successors)
    for node, successors in node_successors.items():
        if successors is None:
            continue
        for successor in successors:
            graph.add_edge(node, successor)
    return graph


def write_filter_id_file(filter_id, path):
    """Writes filter id to disk.

    Args:
        filter_id (str): filter id
        path (Path or str): full path to directory where the filter id file will be written
    """
    with Path(path, ".filter_id").open("w") as filter_id_file:
        filter_id_file.writelines([filter_id + "\n"])


def gather_leaf_data(input_dict, paths, pop=False):
    """Gathers data defined by 'paths' of keys from nested dicts.

    Args:
        input_dict (dict): dict to pop from
        paths (list of tuple): 'paths' of dict keys to leaf entries
        pop (bool): if True, pops the leaf data modifying ''input_dict''

    Returns:
        dict: popped data
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


def get_file_size(size_in_bytes):
    """Returns a human readable string of the size of a file. Given size_in_bytes arg
    is designed as the output of os.path.getsize().

    1 KB = 1024 bytes
    1 MB = 1024*1024 bytes
    1 GB = 1024*1024*1024 bytes

    Args:
        size_in_bytes (int): Size in bytes [B]

    Returns:
        str: Human readable file size
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
