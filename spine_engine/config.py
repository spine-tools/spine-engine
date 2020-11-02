######################################################################################################################
# Copyright (C) 2017-2020 Spine project consortium
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
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

# Tool output directory name
TOOL_OUTPUT_DIR = "output"

# Gimlet default work directory name
GIMLET_WORK_DIR_NAME = "work"
