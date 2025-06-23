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
from typing import ClassVar


@dataclass(frozen=True)
class ProjectItemSpecification:
    item_type: ClassVar[str] = NotImplemented
    name: str
    description: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, specification_dict: dict) -> ProjectItemSpecification:
        kwargs = cls._constructor_kwargs(specification_dict)
        return cls(**kwargs)

    @classmethod
    def _constructor_kwargs(cls, specification_dict: dict) -> dict:
        return {
            "name": specification_dict["name"],
            "description": specification_dict["description"],
        }
