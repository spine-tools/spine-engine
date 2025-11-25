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

"""Functions to (de)serialize stuff."""

import os
from pathlib import Path
import sys
from typing import Literal, TypedDict, TypeGuard
import urllib
from urllib.parse import urljoin
from typing_extensions import NotRequired


def path_in_dir(path: str | Path, directory: str | Path) -> bool:
    """Returns True if the given path is in the given directory.

    Args:
        path: path to test
        directory: directory to test against

    Returns:
        True if path is in directory, False otherwise
    """
    path = Path(path)
    directory = Path(directory)
    if path.drive.casefold() != directory.drive.casefold():
        return False
    return Path(os.path.commonpath((path, directory))) == Path(directory)


class PathDict(TypedDict):
    type: Literal["path", "url", "file_url"]
    path: str
    relative: NotRequired[bool]
    scheme: NotRequired[str]
    query: NotRequired[str]


def is_path_dict_with_file(d: dict) -> TypeGuard[PathDict]:
    return "type" in d and d["type"] in ("path", "file_url") and "path" in d and "relative" in d


def serialize_path(path: str | Path, project_dir: str | Path, is_relative: bool | None = None) -> PathDict:
    """
    Returns a dict representation of the given path.

    Args:
        path: path to serialize
        project_dir: path to the project directory
        is_relative: If True, make path relative to project_dir, if False, keep path absolute;
            if None, make path relative if it points inside project_dir.

    Returns:
        Dictionary representing the given path
    """
    if is_relative is None:
        is_relative = path_in_dir(path, project_dir)
    serialized: PathDict = {
        "type": "path",
        "relative": is_relative,
        "path": (
            os.path.relpath(path, project_dir).replace(os.sep, "/") if is_relative else str(path).replace(os.sep, "/")
        ),
    }
    return serialized


def deserialize_path(serialized: PathDict | str, project_dir: str) -> str:
    """
    Returns a deserialized path or URL.

    Args:
        serialized: a serialized path or URL
        project_dir: path to the project directory

    Returns:
        Path or URL as string
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


def deserialize_remote_path(serialized: PathDict | str, base_path: str) -> str:
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
