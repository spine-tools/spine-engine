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
Application constants.

:author: P. Savolainen (VTT)
:date:   2.1.2018
"""

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
GAMSIDE_EXECUTABLE = _executable("gamside")

# Julia
JULIA_EXECUTABLE = _executable("julia")

# Python
PYTHON_EXECUTABLE = _executable("python" if _on_windows else "python3")
_frozen = getattr(sys, "frozen", False)
_path_to_executable = os.path.dirname(sys.executable if _frozen else __file__)
APPLICATION_PATH = os.path.realpath(_path_to_executable)
# Experimental Python interpreter shipped with Spine Toolbox installation bundle
EMBEDDED_PYTHON = os.path.join(APPLICATION_PATH, "tools", "python.exe")

# Tool output directory name
TOOL_OUTPUT_DIR = "output"
