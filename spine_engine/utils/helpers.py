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
Helpers functions and classes.

:authors: M. Marin (KTH)
:date:   20.11.2019
"""
import os
import sys
import datetime
import time
import json
from jupyter_client.kernelspec import find_kernel_specs
from ..config import PYTHON_EXECUTABLE, JULIA_EXECUTABLE


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


def python_interpreter(app_settings):
    """Returns the full path to Python interpreter depending on
    user's settings and whether the app is frozen or not.

    Args:
        app_settings (QSettings): Application preferences

    Returns:
        str: Path to python executable
    """
    python_path = app_settings.value("appSettings/pythonPath", defaultValue="")
    return resolve_python_interpreter(python_path)


def resolve_python_interpreter(python_path):
    """Solves the full path to Python interpreter and returns it."""
    if python_path != "":
        path = python_path
    else:
        if not getattr(sys, "frozen", False):
            path = sys.executable  # Use current Python
        else:
            # We are frozen
            p = resolve_python_executable_from_path()
            if p != "":
                path = p  # Use Python from PATH
            else:
                path = PYTHON_EXECUTABLE  # Use embedded <app_install_dir>/Tools/python.exe
    return path


def resolve_python_executable_from_path():
    """[Windows only] Returns full path to Python executable in user's PATH env variable.
    If not found, returns an empty string.

    Note: This looks for python.exe so this is Windows only.
    Update needed to PYTHON_EXECUTABLE to make this os independent.
    """
    executable_paths = os.get_exec_path()
    for path in executable_paths:
        if "python" in path.casefold():
            python_candidate = os.path.join(path, "python.exe")
            if os.path.isfile(python_candidate):
                return python_candidate
    return ""


def resolve_julia_executable_from_path():
    """Returns full path to Julia executable in user's PATH env variable.
    If not found, returns an empty string.

    Note: In the long run, we should decide whether this is something we want to do
    because adding julia-x.x./bin/ dir to the PATH is not recommended because this
    also exposes some .dlls to other programs on user's (windows) system. I.e. it
    may break other programs, and this is why the Julia installer does not
    add (and does not even offer the chance to add) Julia to PATH.
    """
    executable_paths = os.get_exec_path()
    for path in executable_paths:
        if "julia" in path.casefold():
            julia_candidate = os.path.join(path, JULIA_EXECUTABLE)
            if os.path.isfile(julia_candidate):
                return julia_candidate
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
        list
    """
    use_embedded_julia = settings.value("appSettings/useEmbeddedJulia", defaultValue="2") == "2"
    if use_embedded_julia:
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
            cmd = [kernel_spec["argv"].pop(0)]
            project_arg = next((arg for arg in kernel_spec["argv"] if arg.startswith("--project=")), "--project=")
            cmd.append(project_arg)
    else:
        julia = settings.value("appSettings/juliaPath", defaultValue="")
        if julia == "":
            julia = resolve_julia_executable_from_path()
        cmd = [julia]
        project = settings.value("appSettings/juliaProjectPath", defaultValue="")
        cmd.append(f"--project={project}")
    return cmd
