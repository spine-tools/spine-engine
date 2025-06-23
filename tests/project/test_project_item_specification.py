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
from dataclasses import dataclass
from spine_engine.project.project_item_specification import ProjectItemSpecification


@dataclass(frozen=True)
class ExampleSpecification(ProjectItemSpecification):
    item_type = "test_item"


class TestProjectItemSpecification:
    def test_serialization(self):
        specification = ExampleSpecification("Spec's name", "Just for testing purposes.")
        serialized = specification.to_dict()
        deserialized = ExampleSpecification.from_dict(serialized)
        assert deserialized == specification
