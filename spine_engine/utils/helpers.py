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
import sys
import datetime
import time
from ..config import PYTHON_EXECUTABLE


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
    if python_path != "":
        path = python_path
    else:
        if not getattr(sys, "frozen", False):
            path = sys.executable  # If not frozen, return the one that is currently used.
        else:
            path = PYTHON_EXECUTABLE  # If frozen, return the one in path
    return path


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
