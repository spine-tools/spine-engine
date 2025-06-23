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
from collections.abc import Callable
from itertools import dropwhile
from typing import ClassVar, Literal, Type
from pydantic import BaseModel, SerializeAsAny, create_model, field_validator, model_serializer, model_validator
from pydantic_core.core_schema import SerializerFunctionWrapHandler

LATEST_PROJECT_VERSION: Literal[13] = 13


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


class ModelSettings(VersionedModel):
    version: ClassVar[int] = 1
    enable_execute_all: bool
    store_external_paths_as_relative: bool


class ProjectItemSpecification(VersionedModel):
    version: ClassVar[int] = 1
    description: str


class FilterSettings(VersionedModel):
    version: ClassVar[int] = 1


class Connection(VersionedModel):
    version: ClassVar[int] = 1
    name: str
    from_: str
    to: str
    filter_settings: FilterSettings

    @model_validator(mode="before")
    @classmethod
    def _fix_from_field_name(cls, data: dict) -> dict:
        if "from" in data:
            data["from_"] = data.pop("from")
        return data

    @model_serializer(mode="wrap")
    def serialize_model(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        serialized = handler(self)
        serialized["from"] = serialized.pop("from_")
        return serialized


class Jump(VersionedModel):
    version: ClassVar[int] = 1


class ProjectItem(VersionedModel):
    version: ClassVar[int] = 1


def build_project_model(item_from_dict: Callable[[dict], ProjectItem]) -> Type[VersionedModel]:
    validators = {"item_validator": field_validator("items", mode="plain")(item_from_dict)}
    Project = create_model(
        "Project",
        version=(ClassVar[int], LATEST_PROJECT_VERSION),
        description=str,
        settings=ModelSettings,
        specification=dict[str, ProjectItemSpecification],
        connections=list[Connection],
        jumps=list[Jump],
        items=dict[str, SerializeAsAny[ProjectItem]],
        __base__=VersionedModel,
        __validators__=validators,
    )
    return Project
