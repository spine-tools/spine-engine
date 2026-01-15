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
from typing import ClassVar, Type
from pydantic import ValidationError
import pytest
from spine_engine.pydantic_models.models import (
    VersionedModel,
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
