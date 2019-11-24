######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for SpineEngine class.

Inspired from tests for spinetoolbox.ExecutionInstance and spinetoolbox.ResourceMap,
and intended to supersed them.

:author: M. Marin (KTH)
:date:   11.9.2019
"""

import unittest
from unittest.mock import NonCallableMagicMock, call
from spine_engine import SpineEngine, SpineEngineEventType


class TestSpineEngine(unittest.TestCase):
    @staticmethod
    def _mock_item(name, resources_forward=None, resources_backward=None, execute_forward=True, execute_backward=True):
        """Returns a mock project item.

        Args:
            name (str)
            resources_forward (list): Forward output_resources return value.
            resources_backward (list): Backward output_resources return value.
            execute_forward (bool): Forward execute return value.
            execute_backward (bool): Backward execute return value.

        Returns:
            NonCallableMagicMock
        """
        if resources_forward is None:
            resources_forward = []
        if resources_backward is None:
            resources_backward = []
        item = NonCallableMagicMock()
        item.name = name
        item.short_name = name.lower().replace(' ', '_')
        item.execute.side_effect = lambda _, direction: {"forward": execute_forward, "backward": execute_backward}[
            direction
        ]
        item.output_resources.side_effect = lambda direction: {
            "forward": resources_forward,
            "backward": resources_backward,
        }[direction]
        return item

    def test_linear_execution_succeeds(self):
        """Tests execution with three items in a line."""
        mock_item_a = self._mock_item("item_a", resources_forward=["url_a_fw"], resources_backward=["url_a_bw"])
        mock_item_b = self._mock_item("item_b", resources_forward=["url_b_fw"], resources_backward=["url_b_bw"])
        mock_item_c = self._mock_item("item_c", resources_forward=["url_c_fw"], resources_backward=["url_c_bw"])
        successors = {"item_a": ["item_b"], "item_b": ["item_c"]}
        engine = SpineEngine([mock_item_a, mock_item_b, mock_item_c], successors)
        item_names = [
            event.item.name for event in engine.run() if event.type_ == SpineEngineEventType.ITEM_EXECUTION_START
        ]
        self.assertEqual(item_names, ["item_a", "item_b", "item_c"])
        item_a_execute_calls = [call(["url_b_bw"], "backward"), call([], "forward")]
        item_b_execute_calls = [call(["url_c_bw"], "backward"), call(["url_a_fw"], "forward")]
        item_c_execute_calls = [call([], "backward"), call(["url_b_fw"], "forward")]
        mock_item_a.execute.assert_has_calls(item_a_execute_calls)
        mock_item_b.execute.assert_has_calls(item_b_execute_calls)
        mock_item_c.execute.assert_has_calls(item_c_execute_calls)

    def test_fork_execution_succeeds(self):
        """Tests execution with three items in a fork."""
        mock_item_a = self._mock_item("item_a", resources_forward=["url_a_fw"], resources_backward=["url_a_bw"])
        mock_item_b = self._mock_item("item_b", resources_forward=["url_b_fw"], resources_backward=["url_b_bw"])
        mock_item_c = self._mock_item("item_c", resources_forward=["url_c_fw"], resources_backward=["url_c_bw"])
        successors = {"item_a": ["item_b", "item_c"]}
        engine = SpineEngine([mock_item_a, mock_item_b, mock_item_c], successors)
        item_names = [
            event.item.name for event in engine.run() if event.type_ == SpineEngineEventType.ITEM_EXECUTION_START
        ]
        self.assertEqual(item_names[0], "item_a")
        self.assertTrue(all(name in item_names[1:] for name in ("item_b", "item_c")))
        item_a_execute_calls = [call(["url_b_bw", "url_c_bw"], "backward"), call([], "forward")]
        item_b_execute_calls = [call([], "backward"), call(["url_a_fw"], "forward")]
        item_c_execute_calls = [call([], "backward"), call(["url_a_fw"], "forward")]
        mock_item_a.execute.assert_has_calls(item_a_execute_calls)
        mock_item_b.execute.assert_has_calls(item_b_execute_calls)
        mock_item_c.execute.assert_has_calls(item_c_execute_calls)

    def test_backward_execution_fails(self):
        """Tests that execution fails going backwards."""
        mock_item_a = self._mock_item("item_a")
        mock_item_b = self._mock_item("item_b", execute_backward=False)
        successors = {"item_a": ["item_b"]}
        engine = SpineEngine([mock_item_a, mock_item_b], successors)
        failed_item_names = [
            event.item.name for event in engine.run() if event.type_ == SpineEngineEventType.ITEM_EXECUTION_FAILURE
        ]
        self.assertEqual(failed_item_names, ["item_b"])


if __name__ == '__main__':
    unittest.main()
