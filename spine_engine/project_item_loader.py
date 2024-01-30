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

"""
Contains :class:`ProjectItemLoader`.

"""
from multiprocessing import Lock
from .load_project_items import load_executable_item_classes, load_item_specification_factories
from .utils.helpers import Singleton


class ProjectItemLoader(metaclass=Singleton):
    """A singleton class for loading project items from multiple processes simultaneously."""

    _specification_factories = {}
    _executable_item_classes = {}
    _specification_factories_lock = Lock()
    _executable_item_classes_lock = Lock()

    def load_item_specification_factories(self, items_module_name):
        """
        Loads the project item specification factories in the standard Toolbox package.

        Args:
            items_module_name (str): name of the Python module that contains the project items

        Returns:
            dict: a map from item type to specification factory
        """
        with self._specification_factories_lock:
            if items_module_name not in self._specification_factories:
                self._specification_factories[items_module_name] = load_item_specification_factories(items_module_name)
        return self._specification_factories[items_module_name]

    def load_executable_item_classes(self, items_module_name):
        """
        Loads the project item executable classes included in the standard Toolbox package.

        Args:
            items_module_name (str): name of the Python module that contains the project items

        Returns:
            dict: a map from item type to the executable item class
        """
        with self._executable_item_classes_lock:
            if items_module_name not in self._executable_item_classes:
                self._executable_item_classes[items_module_name] = load_executable_item_classes(items_module_name)
        return self._executable_item_classes[items_module_name]
