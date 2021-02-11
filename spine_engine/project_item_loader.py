######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains :class:`ProjectItemLoader`.

:author: A. Soininen (VTT)
:date:   11.2.2021
"""
from multiprocessing import Lock
from .load_project_items import load_executable_item_classes, load_item_specification_factories
from .utils.helpers import Singleton


class ProjectItemLoader(metaclass=Singleton):
    """A singleton class for loading project items from multiple processes simultaneously."""

    _specification_factories = []
    _executable_item_classes = []
    _specification_factories_lock = Lock()
    _executable_item_classes_lock = Lock()

    def load_item_specification_factories(self):
        """
        Loads the project item specification factories in the standard Toolbox package.

        Returns:
            dict: a map from item type to specification factory
        """
        with self._specification_factories_lock:
            if not self._specification_factories:
                self._specification_factories.append(load_item_specification_factories())
        return self._specification_factories[0]

    def load_executable_item_classes(self):
        """
        Loads the project item executable classes included in the standard Toolbox package.

        Returns:
            dict: a map from item type to the executable item class
        """
        with self._executable_item_classes_lock:
            if not self._executable_item_classes:
                self._executable_item_classes.append(load_executable_item_classes())
        return self._executable_item_classes[0]
