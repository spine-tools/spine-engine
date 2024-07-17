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

"""
Functions to (de)serialize stuff.

"""

import os
from pathlib import Path
import sys
import urllib
from urllib.parse import urljoin


def path_in_dir(path, directory):
    """Returns True if the given path is in the given directory.

    Args:
        path (str): path to test
        directory (str): directory to test against

    Returns:
        bool: True if path is in directory, False otherwise
    """
    path = Path(path)
    directory = Path(directory)
    if path.drive.casefold() != directory.drive.casefold():
        return False
    return Path(os.path.commonpath((path, directory))) == Path(directory)


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
            "path": (
                os.path.relpath(path, project_dir).replace(os.sep, "/") if is_relative else path.replace(os.sep, "/")
            ),
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
        raise RuntimeError(f"Key '{error}' missing from serialized path")
    raise RuntimeError(f"Cannot deserialize: unknown path type '{path_type}'")


def deserialize_remote_path(serialized, base_path):
    if not isinstance(serialized, dict):
        return serialized
    try:
        path_type = serialized["type"]
        if path_type != "path":
            raise RuntimeError(f"Cannot deserialize remote path type '{path_type}'")
        relative = serialized["relative"]
        if not relative:
            raise RuntimeError("Cannot deserialize non-relative remote path")
        path = serialized["path"]
        return urljoin(base_path, path)
    except KeyError as error:
        raise RuntimeError(f"Key '{error}' missing from serialized url")
