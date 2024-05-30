######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Engine contributors
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

""" Unit tests for ExecutableItem. """
from tempfile import TemporaryDirectory
import unittest
from unittest import mock
from spine_engine import ExecutionDirection
from spine_engine.project_item.executable_item_base import ExecutableItemBase


class TestExecutableItem(unittest.TestCase):
    def setUp(self):
        self._temp_dir = TemporaryDirectory()

    def tearDown(self):
        self._temp_dir.cleanup()

    def test_name(self):
        item = ExecutableItemBase("name", self._temp_dir.name, mock.MagicMock())
        self.assertEqual(item.name, "name")

    def test_output_resources_backward(self):
        item = ExecutableItemBase("name", self._temp_dir.name, mock.MagicMock())
        item._output_resources_backward = mock.MagicMock(return_value=[3, 5, 7])
        item._output_resources_forward = mock.MagicMock()
        self.assertEqual(item.output_resources(ExecutionDirection.BACKWARD), [3, 5, 7])
        item._output_resources_backward.assert_called_once_with()
        item._output_resources_forward.assert_not_called()

    def test_output_resources_forward(self):
        item = ExecutableItemBase("name", self._temp_dir.name, mock.MagicMock())
        item._output_resources_backward = mock.MagicMock()
        item._output_resources_forward = mock.MagicMock(return_value=[3, 5, 7])
        self.assertEqual(item.output_resources(ExecutionDirection.FORWARD), [3, 5, 7])
        item._output_resources_backward.assert_not_called()
        item._output_resources_forward.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
