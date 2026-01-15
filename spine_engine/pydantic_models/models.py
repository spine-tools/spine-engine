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
from itertools import dropwhile
from typing import ClassVar, Type
from pydantic import BaseModel, model_serializer, model_validator
from pydantic_core.core_schema import SerializerFunctionWrapHandler


class VersionedModel(BaseModel):
    version: ClassVar[int] = NotImplemented
    legacies: ClassVar[list[Type[VersionedModel]]] = []

    @model_validator(mode="before")
    @classmethod
    def _migrate_to_latest(cls, data: dict) -> dict:
        if "version" not in data:
            return data
        version = data.pop("version")
        if version < cls.version:
            models = list(dropwhile(lambda model: version != model.version, cls.legacies))
            models[0].model_validate(data)
            for intermediate_model in models[1:]:
                data = intermediate_model.migrate(data)
            data = cls.migrate(data)
        elif version > cls.version:
            raise ValueError(f"version {version} of {cls.__name__} detected while up to {cls.version} is supported")
        return data

    @model_serializer(mode="wrap")
    def serialize_model(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        serialized = handler(self)
        serialized["version"] = self.version
        return serialized

    @classmethod
    def migrate(cls, data: dict) -> dict:
        return data
