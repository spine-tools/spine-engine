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
import pathlib
import sys
import pytest
from spine_engine.project.project import Project
from spine_engine.project.project_dir import ProjectDir


@pytest.fixture()
def base_dir() -> pathlib.Path:
    if sys.platform == "win32":
        return pathlib.WindowsPath(r"c:\toolbox-projects")
    else:
        return pathlib.PosixPath("/home/jbond/toolbox_projects")


class TestProjectDir:
    def test_call(self, base_dir):
        project = Project("My project")
        project_dir = ProjectDir(project, base_dir)
        assert project_dir() == base_dir / "my_project"

    def test_item_data_dir(self, base_dir):
        project = Project("My project")
        project_dir = ProjectDir(project, base_dir)
        assert (
            project_dir.item_data_dir("My project item")
            == base_dir / "my_project" / ".spinetoolbox" / "items" / "my_project_item"
        )

    def test_item_log_dir(self, base_dir):
        project = Project("My project")
        project_dir = ProjectDir(project, base_dir)
        assert (
            project_dir.item_log_dir("My project item")
            == base_dir / "my_project" / ".spinetoolbox" / "items" / "my_project_item" / "logs"
        )
