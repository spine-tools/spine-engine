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

"""Unit tests for `load_project_items` module."""
import os.path
import sys
import unittest
from spine_engine.load_project_items import load_executable_item_classes, load_item_specification_factories
from spine_engine.project_item.executable_item_base import ExecutableItemBase
from spine_engine.project_item.project_item_specification_factory import ProjectItemSpecificationFactory


class TestLoadProjectItems(unittest.TestCase):
    def setUp(self):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "mock_project_items"))

    def tearDown(self):
        sys.path.pop(0)

    def test_load_executable_items(self):
        path = sys.path
        item_classes = load_executable_item_classes("items_module")
        item_types = ("TestItem",)
        for item_type in item_types:
            self.assertIn(item_type, item_classes)
        for item_class in item_classes.values():
            self.assertTrue(issubclass(item_class, ExecutableItemBase))

    def test_load_item_specification_factories(self):
        factories = load_item_specification_factories("items_module")
        self.assertEqual(len(factories), 1)
        self.assertIn("TestItem", factories)
        self.assertTrue(issubclass(factories["TestItem"], ProjectItemSpecificationFactory))


if __name__ == "__main__":
    unittest.main()
