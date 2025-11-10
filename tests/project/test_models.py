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
from typing import Annotated, ClassVar, Literal, Type, Union
from pydantic import Field, ValidationError
import pytest
from spine_engine.project.models import (
    ProjectItem,
    ProjectItemSpecification,
    ProjectSettings,
    VersionedModel,
    build_project_model,
)


class _Version0(VersionedModel):
    version: ClassVar[int] = 0
    data1: int


class _Version1(VersionedModel):
    version: ClassVar[int] = 1
    data2: str

    @classmethod
    def migrate(cls, data: dict) -> dict:
        return {"data2": str(data["data1"])}


class _CurrentVersion(VersionedModel):
    version: ClassVar[int] = 2
    legacies: ClassVar[list[Type[VersionedModel]]] = [_Version0, _Version1]
    data3: int

    @classmethod
    def migrate(cls, data: dict) -> dict:
        return {"data3": int(data["data2"])}


class _FutureVersion(VersionedModel):
    version: ClassVar[int] = 3
    data4: str


class TestVersionedModel:
    def test_current_version_serialization_and_validation(self):
        original = _CurrentVersion(data3=23)
        serialized = original.model_dump(mode="json")
        deserialized = _CurrentVersion.model_validate(serialized)
        assert deserialized == original

    def test_migration_from_previous_version(self):
        original = _Version1(data2="23")
        serialized = original.model_dump(mode="json")
        deserialized = _CurrentVersion.model_validate(serialized)
        assert deserialized == _CurrentVersion(data3=23)

    def test_migration_from_first_version(self):
        original = _Version0(data1=23)
        serialized = original.model_dump(mode="json")
        deserialized = _CurrentVersion.model_validate(serialized)
        assert deserialized == _CurrentVersion(data3=23)

    def test_validate_data_before_migration(self):
        with pytest.raises(ValidationError):
            _CurrentVersion.model_validate({"version": 0, "data1": "N/A"})

    def test_unsupported_version_raises_validation_error(self):
        future = _FutureVersion(data4="23")
        serialized = future.model_dump(mode="json")
        with pytest.raises(ValidationError, match="version 3 of _CurrentVersion detected while up to 2 is supported"):
            _CurrentVersion.model_validate(serialized)


class MyFirstProjectItem(ProjectItem):
    version: ClassVar[int] = 1
    type: Literal["1st"] = "1st"
    option: str


class MySecondProjectItem(ProjectItem):
    version: ClassVar[int] = 1
    type: Literal["2nd"] = "2nd"
    count: int
    stuff: list[str]


class MySpecification(ProjectItemSpecification):
    version: ClassVar[int] = 1
    type: Literal["first_spec"] = "first_spec"
    settings: dict[str, str]


class TestBuildProjectModel:
    def test_with_project_items(self):
        ItemUnion = Annotated[Union[MyFirstProjectItem, MySecondProjectItem], Field(discriminator="type")]
        SpecificationUnion = Annotated[Union[MySpecification], Field(discriminator="type")]
        Project = build_project_model(ItemUnion, SpecificationUnion)
        project_settings = ProjectSettings(enable_execute_all=True, store_external_paths_as_relative=False)
        first_item = MyFirstProjectItem(
            description="My first item instance.",
            x=2.3,
            y=-2.3,
            option="no",
        )
        second_item = MySecondProjectItem(
            description="My second item instance.",
            x=-2.3,
            y=2.3,
            count=5,
            stuff=["a", "b", "d"],
        )
        project = Project(
            description="My project model.",
            settings=project_settings,
            specifications={},
            connections=[],
            jumps=[],
            items={"first": first_item, "second": second_item},
        )
        serialized = project.model_dump(mode="json")
        deserialized = Project.model_validate(serialized)
        assert deserialized == project

    def test_with_specifications(self):
        ItemUnion = Annotated[Union[MyFirstProjectItem, MySecondProjectItem], Field(discriminator="type")]
        SpecificationUnion = Annotated[Union[MySpecification], Field(discriminator="type")]
        Project = build_project_model(ItemUnion, SpecificationUnion)
        project_settings = ProjectSettings(enable_execute_all=True, store_external_paths_as_relative=False)
        specification1 = MySpecification(
            description="A curious general purpose item.", settings={"size": "big", "mass": "small"}
        )
        specification2 = MySpecification(description="Another mystic thing.", settings={"correctness": "medium"})
        project = Project(
            description="My project model.",
            settings=project_settings,
            specifications={"first": specification1, "second": specification2},
            connections=[],
            jumps=[],
            items={},
        )
        serialized = project.model_dump(mode="json")
        deserialized = Project.model_validate(serialized)
        assert deserialized == project
