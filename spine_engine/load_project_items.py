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
Functions to load project item modules.

:author: A. Soininen (VTT)
:date:   29.4.2020
"""
import pathlib
import importlib
import importlib.util


def load_item_specification_factories():
    """
    Loads the project item specification factories in the ``spine_items`` package.

    Returns:
        dict: a map from item type to specification factory
    """
    import spine_items  # pylint: disable=import-outside-toplevel

    items_root = pathlib.Path(spine_items.__file__).parent
    factories = dict()
    for child in items_root.iterdir():
        if child.is_dir() and child.joinpath("specification_factory.py").exists() or \
                (child.is_dir() and child.joinpath("specification_factory.pyc").exists()):
            spec = importlib.util.find_spec(f"spine_items.{child.stem}.specification_factory")
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            if hasattr(m, "SpecificationFactory"):
                item_type = m.SpecificationFactory.item_type()
                factories[item_type] = m.SpecificationFactory
    return factories


def load_executable_item_classes():
    """
    Loads the project item executable classes included in the ``spine_items`` package.

    Returns:
        dict: a map from item type to the executable item class
    """
    import spine_items  # pylint: disable=import-outside-toplevel

    items_root = pathlib.Path(spine_items.__file__).parent
    classes = dict()
    for child in items_root.iterdir():
        if child.is_dir() and child.joinpath("executable_item.py").exists() or \
                (child.is_dir() and child.joinpath("executable_item.pyc").exists()):
            spec = importlib.util.find_spec(f"spine_items.{child.stem}.executable_item")
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            if hasattr(m, "ExecutableItem"):
                item_class = m.ExecutableItem
                item_type = item_class.item_type()
                classes[item_type] = item_class
    return classes
