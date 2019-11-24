######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains event-related classes.

:authors: M. Marin (KTH)
:date:   20.11.2019
"""

from enum import Enum


class SpineEngineEventType(Enum):
    ITEM_EXECUTION_START = 1
    ITEM_EXECUTION_FAILURE = 2


class SpineEngineEvent:
    def __init__(self, type_=None, item=None):
        self.type_ = type_
        self.item = item
