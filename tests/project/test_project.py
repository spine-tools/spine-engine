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
import pytest
from spine_engine.project.backward_jump import BackwardJump
from spine_engine.project.connection import Connection
from spine_engine.project.exception import ProjectException
from spine_engine.project.project import Project
from spine_engine.project.project_item import ProjectItem
from spine_engine.project.project_item_specification import ProjectItemSpecification


class TestProject:
    def test_name(self):
        project = Project("my project")
        assert project.name() == "my project"

    def test_description(self):
        project = Project("my project", "A test project.")
        assert project.description() == "A test project."

    def test_add_project_item(self):
        project = Project("my project")
        item = ProjectItem()
        project.add_item("A", item)
        assert len(list(project.item_name_iter())) == 1
        assert project.item("A") is item

    def test_adding_items_with_duplicate_names_raises(self):
        project = Project("my project")
        project.add_item("A", ProjectItem())
        with pytest.raises(ProjectException, match="^project already contains A$"):
            project.add_item("A", ProjectItem())

    def test_rename_unconnected_item(self):
        project = Project("my project")
        item = ProjectItem()
        project.add_item("A", item)
        project.rename_item("A", "B")
        assert len(list(project.item_name_iter())) == 1
        assert project.item("B") is item

    def test_rename_item_with_predecessor_preserves_the_connection(self):
        project = Project("my project")
        project.add_item("predecessor", ProjectItem())
        project.add_item("A", ProjectItem())
        connection = Connection()
        project.add_connection("predecessor", "A", connection)
        project.rename_item("A", "B")
        assert project.connection("predecessor", "B") is connection

    def test_rename_item_with_successor_preserves_the_connection(self):
        project = Project("my project")
        project.add_item("successor", ProjectItem())
        project.add_item("A", ProjectItem())
        connection = Connection()
        project.add_connection("A", "successor", connection)
        project.rename_item("A", "B")
        assert project.connection("B", "successor") is connection

    def test_renaming_item_to_existing_item_raises(self):
        project = Project("my project")
        project.add_item("A", ProjectItem())
        project.add_item("B", ProjectItem())
        with pytest.raises(ProjectException, match="^project already contains B$"):
            project.rename_item("A", "B")

    def test_remove_item(self):
        project = Project("my project")
        project.add_item("A", ProjectItem())
        project.remove_item("A")
        with pytest.raises(ProjectException, match="^no such item$"):
            project.item("A")

    def test_removing_nonexistent_item_raises(self):
        project = Project("my project")
        with pytest.raises(ProjectException, match="^no such item$"):
            project.item("A")

    def test_connect_items(self):
        project = Project("test project")
        project.add_item("predecessor", ProjectItem())
        project.add_item("successor", ProjectItem())
        connection = Connection()
        project.add_connection("predecessor", "successor", connection)
        assert project.connection("predecessor", "successor") is connection

    def test_connecting_non_existent_source_raises(self):
        project = Project("test project")
        project.add_item("successor", ProjectItem())
        with pytest.raises(ProjectException, match="^no such source item$"):
            project.add_connection("predecessor", "successor", Connection())

    def test_connecting_non_existent_target_raises(self):
        project = Project("test project")
        project.add_item("predecessor", ProjectItem())
        with pytest.raises(ProjectException, match="^no such target item$"):
            project.add_connection("predecessor", "successor", Connection())

    def test_connecting_item_to_self_raises(self):
        project = Project("test project")
        project.add_item("A", ProjectItem())
        with pytest.raises(ProjectException, match="^cannot connect A to itself$"):
            project.add_connection("A", "A", Connection())

    def test_adding_duplicate_connection_raises(self):
        project = Project("test project")
        project.add_item("predecessor", ProjectItem())
        project.add_item("successor", ProjectItem())
        connection = Connection()
        project.add_connection("predecessor", "successor", connection)
        with pytest.raises(ProjectException, match="^predecessor and successor are already connected"):
            project.add_connection("predecessor", "successor", connection)

    def test_adding_cyclic_connection_raises(self):
        project = Project("test project")
        project.add_item("predecessor", ProjectItem())
        project.add_item("successor", ProjectItem())
        project.add_connection("predecessor", "successor", Connection())
        with pytest.raises(ProjectException, match="^connecting successor to predecessor would create a cycle$"):
            project.add_connection("successor", "predecessor", Connection())
        with pytest.raises(ProjectException, match="^no such connection$"):
            project.connection("successor", "predecessor")

    def test_remove_connection(self):
        project = Project("test project")
        project.add_item("predecessor", ProjectItem())
        project.add_item("successor", ProjectItem())
        project.add_connection("predecessor", "successor", Connection())
        project.remove_connection("predecessor", "successor")
        with pytest.raises(ProjectException, match="^no such connection$"):
            project.connection("predecessor", "successor")

    def test_removing_nonexistent_connection_raises(self):
        project = Project("test project")
        project.add_item("predecessor", ProjectItem())
        project.add_item("successor", ProjectItem())
        with pytest.raises(ProjectException, match="^no such connection$"):
            project.remove_connection("predecessor", "successor")

    def test_add_backward_jump(self):
        project = Project("test project")
        jump = BackwardJump()
        project.add_item("predecessor", ProjectItem())
        project.add_item("successor", ProjectItem())
        project.add_connection("predecessor", "successor", Connection())
        project.add_backward_jump("successor", "predecessor", jump)
        assert project.backward_jump("successor", "predecessor") is jump

    def test_remove_backward_jump(self):
        project = Project("test project")
        jump = BackwardJump()
        project.add_item("predecessor", ProjectItem())
        project.add_item("successor", ProjectItem())
        project.add_connection("predecessor", "successor", Connection())
        project.add_backward_jump("successor", "predecessor", jump)
        project.remove_backward_jump("successor", "predecessor")
        with pytest.raises(ProjectException, match="^no such jump$"):
            project.backward_jump("successor", "predecessor")

    def test_add_specification(self):
        project = Project("test project")
        specification = ExampleSpecification("Test spec")
        project.add_item_specification(specification)
        assert project.item_specification("test_item", "Test spec") is specification

    def test_cannot_add_duplicate_specifications(self):
        project = Project("test project")
        specification1 = ExampleSpecification("Test spec")
        specification2 = ExampleSpecification("Test spec")
        project.add_item_specification(specification1)
        with pytest.raises(ProjectException, match="^project already contains Test spec$"):
            project.add_item_specification(specification2)

    def test_remove_specification(self):
        project = Project("test project")
        specification = ExampleSpecification("Test spec")
        project.add_item_specification(specification)
        project.remove_item_specification("test_item", "Test spec")
        with pytest.raises(ProjectException, match="^no such specification"):
            project.item_specification("test_item", "Test spec")

    def test_removing_non_existent_specification_raises(self):
        project = Project("test project")
        specification = ExampleSpecification("Test spec")
        project.add_item_specification(specification)
        with pytest.raises(ProjectException, match="^no such specification"):
            project.remove_item_specification("test_item", "Not my cup of tea")


@dataclass(frozen=True)
class ExampleSpecification(ProjectItemSpecification):
    item_type = "test_item"
