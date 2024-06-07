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

"""Spine Engine constants."""
import os
import sys

_on_windows = sys.platform == "win32"


def _executable(name):
    """Appends a .exe extension to `name` on Windows platform."""
    if _on_windows:
        return name + ".exe"
    return name


# GAMS
GAMS_EXECUTABLE = _executable("gams")

# Julia
JULIA_EXECUTABLE = _executable("julia")


# Python
def is_frozen():
    """Checks if we are currently running as frozen bundle.

    Returns:
        bool: True if we are frozen, False otherwise
    """
    return getattr(sys, "frozen", False)


def is_pyinstaller_bundle():
    """Checks if we are in a PyInstaller bundle.

    Returns:
        bool: True if the current bundle has been build by PyInstaller, False otherwise
    """
    return hasattr(sys, "_MEIPASS")


PYTHON_EXECUTABLE = _executable("python" if _on_windows else "python3")
_frozen = getattr(sys, "frozen", False)
_path_to_executable = os.path.dirname(sys.executable if _frozen else __file__)
APPLICATION_PATH = os.path.realpath(_path_to_executable)
# Python interpreter shipped with bundle
BUNDLE_DIR = "Python"
if is_frozen():
    if is_pyinstaller_bundle():
        EMBEDDED_PYTHON = os.path.join(sys._MEIPASS, BUNDLE_DIR, PYTHON_EXECUTABLE)
    else:
        EMBEDDED_PYTHON = os.path.join(APPLICATION_PATH, BUNDLE_DIR, PYTHON_EXECUTABLE)
else:
    EMBEDDED_PYTHON = None


# Tool output directory name
TOOL_OUTPUT_DIR = "output"
