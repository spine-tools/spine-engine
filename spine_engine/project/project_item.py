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
class ProjectItem:
    item_type: ClassVar[str] = NotImplemented
    description: str = ""

    def to_dict(self) -> dict:
        return {}

    @classmethod
    def from_dict(cls, item_dict: dict) -> ProjectItem:
        kwargs = cls._constructor_kwargs(item_dict)
        return cls(**kwargs)

    @classmethod
    def _constructor_kwargs(cls, item_dict: dict) -> dict:
        return {}


@dataclass(frozen=True)
class SpecificItem(ProjectItem):
    specification_name: str | None = None

    def to_dict(self) -> dict:
        item_dict = super().to_dict()
        item_dict["specification_name"] = self.specification_name
        return item_dict

    @classmethod
    def _constructor_kwargs(cls, item_dict: dict) -> dict:
        kwargs = ProjectItem._constructor_kwargs(item_dict)
        kwargs["specification_name"] = item_dict["specification_name"]
        return kwargs


@dataclass(frozen=True)
class ExecutionGroupItem(SpecificItem):
    group_id: str | None = None

    def to_dict(self) -> dict:
        item_dict = super().to_dict()
        item_dict["group_id"] = self.group_id
        return item_dict

    @classmethod
    def _constructor_kwargs(cls, item_dict: dict) -> dict:
        kwargs = SpecificItem._constructor_kwargs(item_dict)
        kwargs["group_id"] = item_dict["group_id"]
        return kwargs
