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
from dataclasses import dataclass, field
from typing import Optional
from spine_engine.project.filter_settings import FilterSettings


@dataclass
class Connection:
    filter_settings: Optional[FilterSettings] = field(default=None)

    def to_dict(self) -> dict:
        return {} if self.filter_settings is None else {"filter_settings": self.filter_settings.to_dict()}

    @classmethod
    def from_dict(cls, connection_dict: dict) -> Connection:
        return cls(**cls._constructor_kwargs(connection_dict))

    @classmethod
    def _constructor_kwargs(cls, connection_dict: dict) -> dict:
        if "filter_settings" in connection_dict:
            return {"filter_settings": FilterSettings.from_dict(connection_dict["filter_settings"])}
        return {}
