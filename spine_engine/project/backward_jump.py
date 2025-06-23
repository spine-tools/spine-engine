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
from __future__ import annotations
from dataclasses import dataclass


@dataclass
class BackwardJump:
    def to_dict(self) -> dict:
        return {}

    @classmethod
    def from_dict(cls, item_dict: dict) -> BackwardJump:
        return cls(**cls._constructor_kwargs(item_dict))

    @classmethod
    def _constructor_kwargs(cls, item_dict: dict) -> dict:
        return {}
