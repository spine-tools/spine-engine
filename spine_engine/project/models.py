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
from typing import ClassVar, Literal, Type, TypeAlias
from pydantic import BaseModel, create_model, model_serializer, model_validator
from pydantic_core.core_schema import SerializerFunctionWrapHandler

LATEST_PROJECT_VERSION: Literal[14] = 14


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


class ProjectSettings(VersionedModel):
    version: ClassVar[int] = 1
    enable_execute_all: bool
    store_external_paths_as_relative: bool


class ProjectItem(VersionedModel):
    description: str
    x: float
    y: float


class ProjectItemSpecification(VersionedModel):
    description: str


FilterType = Literal["alternative", "scenario"]


class FilterSettings(BaseModel):
    known_filters: dict[str, dict[str, dict[FilterType, bool]]]
    auto_online: bool
    enabled_filter_types: dict[FilterType, bool]


ConnectorPosition: TypeAlias = Literal["top", "bottom", "left", "right"]


class ConnectionBase(VersionedModel):
    name: str
    from_: tuple[str, ConnectorPosition]
    to: tuple[str, ConnectorPosition]

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


class Connection(ConnectionBase):
    version: ClassVar[int] = 1
    filter_settings: FilterSettings


class CmdLineArg(BaseModel):
    type: Literal["literal", "resource"]
    arg: str


class Jump(ConnectionBase):
    version: ClassVar[int] = 1
    condition: str
    cmd_line_args: list[CmdLineArg]


class Path(BaseModel):
    type: Literal["path", "url", "file_url"]
    path: str
    relative: bool | None
    scheme: str | None
    query: str | None


def build_project_model(ItemUnion: TypeAlias, SpecificationUnion: TypeAlias) -> Type[VersionedModel]:
    Project = create_model(
        "Project",
        version=(ClassVar[int], LATEST_PROJECT_VERSION),
        description=str,
        settings=ProjectSettings,
        specifications=dict[str, SpecificationUnion],
        connections=list[Connection],
        jumps=list[Jump],
        items=dict[str, ItemUnion],
        __base__=VersionedModel,
    )
    return Project
