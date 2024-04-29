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
""" Functions to load project item modules. """
import importlib


def load_item_specification_factories(items_package_name):
    """
    Loads the project item specification factories in given project item package.

    Args:
        items_package_name (str): name of the package that contains the project items

    Returns:
        dict: a map from item type to specification factory
    """
    items = importlib.import_module(items_package_name)
    return items.item_specification_factories()


def load_executable_item_classes(items_package_name):
    """
    Loads the project item executable classes included in given project item package.

    Args:
        items_package_name (str): name of the package that contains the project items

    Returns:
        dict: a map from item type to the executable item class
    """
    items = importlib.import_module(items_package_name)
    return items.executable_items()
