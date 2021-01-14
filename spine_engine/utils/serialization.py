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
Functions to (de)serialize stuff.

:authors: P. Savolainen (VTT)
:date:   10.1.2018
"""

import os
import sys
import urllib


def path_in_dir(path, directory):
    """Returns True if the given path is in the given directory."""
    try:
        retval = os.path.samefile(os.path.commonpath((path, directory)), directory)
    except ValueError:
        return False
    return retval


def serialize_path(path, project_dir):
    """
    Returns a dict representation of the given path.

    If path is in project_dir, converts the path to relative.

    Args:
        path (str): path to serialize
        project_dir (str): path to the project directory

    Returns:
        dict: Dictionary representing the given path
    """
    is_relative = path_in_dir(path, project_dir)
    serialized = {
        "type": "path",
        "relative": is_relative,
        "path": os.path.relpath(path, project_dir).replace(os.sep, "/") if is_relative else path.replace(os.sep, "/"),
    }
    return serialized


def serialize_url(url, project_dir):
    """
    Return a dict representation of the given URL.

    If the URL is a file that is in project dir, the URL is converted to a relative path.

    Args:
        url (str): a URL to serialize
        project_dir (str): path to the project directory

    Returns:
        dict: Dictionary representing the URL
    """
    parsed = urllib.parse.urlparse(url)
    path = urllib.parse.unquote(parsed.path)
    if sys.platform == "win32":
        path = path[1:]  # Remove extra '/' from the beginning
    if os.path.isfile(path):
        is_relative = path_in_dir(path, project_dir)
        serialized = {
            "type": "file_url",
            "relative": is_relative,
            "path": os.path.relpath(path, project_dir).replace(os.sep, "/")
            if is_relative
            else path.replace(os.sep, "/"),
            "scheme": parsed.scheme,
        }
        if parsed.query:
            serialized["query"] = parsed.query
    else:
        serialized = {"type": "url", "relative": False, "path": url}
    return serialized


def deserialize_path(serialized, project_dir):
    """
    Returns a deserialized path or URL.

    Args:
        serialized (dict): a serialized path or URL
        project_dir (str): path to the project directory

    Returns:
        str: Path or URL as string
    """
    if not isinstance(serialized, dict):
        return serialized
    try:
        path_type = serialized["type"]
        if path_type == "path":
            path = serialized["path"]
            return os.path.normpath(os.path.join(project_dir, path) if serialized["relative"] else path)
        if path_type == "file_url":
            path = serialized["path"]
            if serialized["relative"]:
                path = os.path.join(project_dir, path)
            path = os.path.normpath(path)
            query = serialized.get("query", "")
            if query:
                query = "?" + query
            return serialized["scheme"] + ":///" + path + query
        if path_type == "url":
            return serialized["path"]
    except KeyError as error:
        raise RuntimeError("Key missing from serialized path: {}".format(error))
    raise RuntimeError("Cannot deserialize: unknown path type '{}'.".format(path_type))


def serialize_checked_states(files, project_path):
    """Serializes file paths and adds a boolean value
    for each, which indicates whether the path is
    selected or not. Used in saving checked file states to
    project.json.

    Args:
        files (list): List of absolute file paths
        project_path (str): Absolute project directory path

    Returns:
        list: List of serialized paths with a boolean value
    """
    return [[serialize_path(item.label, project_path), item.selected] for item in files]


def deserialize_checked_states(serialized, project_path):
    """Reverse operation for serialize_checked_states above.
    Returns absolute file paths with their check state as boolean.

    Args:
        serialized (list): List of serialized paths with a boolean value
        project_path (str): Absolute project directory path

    Returns:
        dict: Dictionary with paths as keys and boolean check states as value
    """
    if not serialized:
        return dict()
    deserialized = dict()
    for serialized_label, checked in serialized:
        label = deserialize_path(serialized_label, project_path)
        deserialized[label] = checked
    return deserialized
