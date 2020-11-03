######################################################################################################################
# Copyright (C) 2017-2020 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Functions to load project item modules.

:author: A. Soininen (VTT)
:date:   29.4.2020
"""
import pathlib
import importlib
import importlib.util
from .utils.helpers import Singleton


class ProjectItemLoader(metaclass=Singleton):
    _specification_factories = {}
    _executable_item_classes = {}

    def load_item_specification_factories(self):
        """
        Loads the project item specification factories in the standard Toolbox package.

        Returns:
            dict: a map from item type to specification factory
        """
        if self not in self._specification_factories:
            import spine_items

            items_root = pathlib.Path(spine_items.__file__).parent
            self._specification_factories[self] = factories = dict()
            for child in items_root.iterdir():
                if child.is_dir() and child.joinpath("specification_factory_qt_free.py").exists():
                    spec = importlib.util.find_spec(f"spine_items.{child.stem}.specification_factory_qt_free")
                elif child.is_dir() and child.joinpath("specification_factory.py").exists():
                    spec = importlib.util.find_spec(f"spine_items.{child.stem}.specification_factory")
                else:
                    continue
                m = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(m)
                if hasattr(m, "SpecificationFactory"):
                    item_type = m.SpecificationFactory.item_type()
                    factories[item_type] = m.SpecificationFactory
        return self._specification_factories[self]

    def load_executable_item_classes(self):
        """
        Loads the project item executable classes included in the standard Toolbox package.

        Returns:
            dict: a map from item type to the executable item class
        """
        if self not in self._executable_item_classes:
            import spine_items

            items_root = pathlib.Path(spine_items.__file__).parent
            self._executable_item_classes[self] = executable_items = dict()
            for child in items_root.iterdir():
                if child.is_dir() and child.joinpath("executable_item_qt_free.py").exists():
                    spec = importlib.util.find_spec(f"spine_items.{child.stem}.executable_item_qt_free")
                elif child.is_dir() and child.joinpath("executable_item.py").exists():
                    spec = importlib.util.find_spec(f"spine_items.{child.stem}.executable_item")
                else:
                    continue
                m = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(m)
                if hasattr(m, "ExecutableItem"):
                    item_class = m.ExecutableItem
                    item_type = item_class.item_type()
                    executable_items[item_type] = item_class
        return self._executable_item_classes[self]
