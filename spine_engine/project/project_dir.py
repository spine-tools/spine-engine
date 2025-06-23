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
from spine_engine.project.project import Project
from spine_engine.utils.helpers import shorten

SPINETOOLBOX_DIR = pathlib.Path(".spinetoolbox")
ITEMS_DIR = SPINETOOLBOX_DIR / "items"
LOCAL_DIR = SPINETOOLBOX_DIR / "local"


class ProjectDir:
    def __init__(self, base_dir: pathlib.Path):
        self._base_dir = base_dir

    def item_data_dir(self, name: str) -> pathlib.Path:
        return self._base_dir / ITEMS_DIR / shorten(name)

    def item_log_dir(self, name: str) -> pathlib.Path:
        return self.item_data_dir(name) / "logs"

    def ensure_directories_exist(self, project: Project) -> None:
        for name in project.item_name_iter():
            self.item_data_dir(name).mkdir(parents=True, exist_ok=True)
            self.item_log_dir(name).mkdir(parents=True, exist_ok=True)
