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
and intended to supersede them.

:author: M. Marin (KTH)
:date:   11.9.2019
"""

import unittest
from unittest import mock
from unittest.mock import MagicMock, NonCallableMagicMock, call
from spine_engine import ExecutionDirection, SpineEngine, SpineEngineState
from spinedb_api.filters.scenario_filter import scenario_filter_config
from spinedb_api.filters.execution_filter import execution_filter_config
from spinedb_api.filters.tools import filter_configs


class _MockProjectItemResource(MagicMock):

    url = "sqlite:///"

    def clone(self, *args, **kwargs):
        return self

    @property
    def label(self):
        return self


class TestSpineEngine(unittest.TestCase):
    @staticmethod
    def _mock_item(name, resources_forward=None, resources_backward=None, execute_outcome=True):
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
        item.execute.side_effect = lambda _, __: execute_outcome
        item.skip_execution = MagicMock()
        for r in resources_forward + resources_backward:
            r.provider = item
        item.output_resources.side_effect = lambda direction: {
            ExecutionDirection.FORWARD: resources_forward,
            ExecutionDirection.BACKWARD: resources_backward,
        }[direction]
        return item

    def setUp(self):
        self.url_a_fw = _MockProjectItemResource()
        self.url_b_fw = _MockProjectItemResource()
        self.url_c_fw = _MockProjectItemResource()
        self.url_a_bw = _MockProjectItemResource()
        self.url_b_bw = _MockProjectItemResource()
        self.url_c_bw = _MockProjectItemResource()

    def test_linear_execution_succeeds(self):
        """Tests execution with three items in a line."""
        mock_item_a = self._mock_item("item_a", resources_forward=[self.url_a_fw], resources_backward=[self.url_a_bw])
        mock_item_b = self._mock_item("item_b", resources_forward=[self.url_b_fw], resources_backward=[self.url_b_bw])
        mock_item_c = self._mock_item("item_c", resources_forward=[self.url_c_fw], resources_backward=[self.url_c_bw])
        items = {"item_a": mock_item_a, "item_b": mock_item_b, "item_c": mock_item_c}
        successors = {"item_a": ["item_b"], "item_b": ["item_c"]}
        execution_permits = {"item_a": True, "item_b": True, "item_c": True}
        engine = SpineEngine(items=items, node_successors=successors, execution_permits=execution_permits)
        engine._make_item = lambda name, *args: engine._items[name]
        with mock.patch("spine_engine.spine_engine.append_filter_config"):
            engine.run()
        item_a_execute_calls = [call([], [self.url_b_bw])]
        item_b_execute_calls = [call([self.url_a_fw], [self.url_c_bw])]
        item_c_execute_calls = [call([self.url_b_fw], [])]
        mock_item_a.execute.assert_has_calls(item_a_execute_calls)
        mock_item_a.skip_execution.assert_not_called()
        mock_item_b.execute.assert_has_calls(item_b_execute_calls)
        mock_item_b.skip_execution.assert_not_called()
        mock_item_c.execute.assert_has_calls(item_c_execute_calls)
        mock_item_c.skip_execution.assert_not_called()
        self.assertEqual(engine.state(), SpineEngineState.COMPLETED)

    def test_fork_execution_succeeds(self):
        """Tests execution with three items in a fork."""
        mock_item_a = self._mock_item("item_a", resources_forward=[self.url_a_fw], resources_backward=[self.url_a_bw])
        mock_item_b = self._mock_item("item_b", resources_forward=[self.url_b_fw], resources_backward=[self.url_b_bw])
        mock_item_c = self._mock_item("item_c", resources_forward=[self.url_c_fw], resources_backward=[self.url_c_bw])
        items = {"item_a": mock_item_a, "item_b": mock_item_b, "item_c": mock_item_c}
        successors = {"item_a": ["item_b", "item_c"]}
        execution_permits = {"item_a": True, "item_b": True, "item_c": True}
        engine = SpineEngine(items=items, node_successors=successors, execution_permits=execution_permits)
        engine._make_item = lambda name, *args: engine._items[name]
        with mock.patch("spine_engine.spine_engine.append_filter_config"):
            engine.run()
        item_a_execute_calls = [call([], [self.url_b_bw, self.url_c_bw])]
        item_b_execute_calls = [call([self.url_a_fw], [])]
        item_c_execute_calls = [call([self.url_a_fw], [])]
        mock_item_a.execute.assert_has_calls(item_a_execute_calls)
        mock_item_a.skip_execution.assert_not_called()
        mock_item_b.execute.assert_has_calls(item_b_execute_calls)
        mock_item_b.skip_execution.assert_not_called()
        mock_item_c.execute.assert_has_calls(item_c_execute_calls)
        mock_item_c.skip_execution.assert_not_called()
        self.assertEqual(engine.state(), SpineEngineState.COMPLETED)

    def test_execution_permits(self):
        """Tests that the middle item of an item triplet is not executed when its execution permit is False."""
        mock_item_a = self._mock_item("item_a", resources_forward=[self.url_a_fw], resources_backward=[self.url_a_bw])
        mock_item_b = self._mock_item("item_b", resources_forward=[self.url_b_fw], resources_backward=[self.url_b_bw])
        mock_item_c = self._mock_item("item_c", resources_forward=[self.url_c_fw], resources_backward=[self.url_c_bw])
        items = {"item_a": mock_item_a, "item_b": mock_item_b, "item_c": mock_item_c}
        successors = {"item_a": ["item_b"], "item_b": ["item_c"]}
        execution_permits = {"item_a": True, "item_b": False, "item_c": True}
        engine = SpineEngine(items=items, node_successors=successors, execution_permits=execution_permits)
        engine._make_item = lambda name, *args: engine._items[name]
        with mock.patch("spine_engine.spine_engine.append_filter_config"):
            engine.run()
        item_a_execute_calls = [call([], [self.url_b_bw])]
        item_b_skip_execution_calls = [call([self.url_a_fw], [self.url_c_bw])]
        item_c_execute_calls = [call([self.url_b_fw], [])]
        mock_item_a.execute.assert_has_calls(item_a_execute_calls)
        mock_item_a.skip_execution.assert_not_called()
        mock_item_b.execute.assert_not_called()
        mock_item_b.skip_execution.assert_has_calls(item_b_skip_execution_calls)
        mock_item_b.output_resources.assert_called()
        mock_item_c.execute.assert_has_calls(item_c_execute_calls)
        mock_item_c.skip_execution.assert_not_called()
        self.assertEqual(engine.state(), SpineEngineState.COMPLETED)

    def test_filter_stacks(self):
        """Tests filter stacks are properly applied."""
        url_a_fw = _MockProjectItemResource()
        url_b_fw = _MockProjectItemResource()
        url_c_bw = _MockProjectItemResource()
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[])
        mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[])
        mock_item_c = self._mock_item("item_c", resources_forward=[], resources_backward=[url_c_bw])
        items = {"item_a": mock_item_a, "item_b": mock_item_b, "item_c": mock_item_c}
        successors = {"item_a": ["item_b"], "item_b": ["item_c"]}
        execution_permits = {"item_a": True, "item_b": True, "item_c": True}
        filter_stacks = {(url_a_fw, "item_b"): [(scenario_filter_config("scen1"),), (scenario_filter_config("scen2"),)]}
        engine = SpineEngine(
            items=items, node_successors=successors, execution_permits=execution_permits, filter_stacks=filter_stacks
        )
        engine._make_item = lambda name, *args: engine._items[name]
        forward_stacks = []
        backward_stacks = []

        def item_b_execute(
            forward_resources, backward_resources, forward_stacks=forward_stacks, backward_stacks=backward_stacks
        ):
            forward_stacks += [filter_configs(r.url) for r in forward_resources]
            backward_stacks += [filter_configs(r.url) for r in backward_resources]
            return True

        url_a_fw.clone = lambda *args, **kwargs: _MockProjectItemResource()
        url_c_bw.clone = lambda *args, **kwargs: _MockProjectItemResource()
        mock_item_b.execute.side_effect = item_b_execute
        engine.run()
        self.assertEqual(len(forward_stacks), 2)
        self.assertEqual(len(backward_stacks), 2)
        self.assertIn([scenario_filter_config("scen1")], forward_stacks)
        self.assertIn([scenario_filter_config("scen2")], forward_stacks)
        self.assertIn([execution_filter_config({"execution_item": "item_b", "scenarios": ["scen1"]})], backward_stacks)
        self.assertIn([execution_filter_config({"execution_item": "item_b", "scenarios": ["scen2"]})], backward_stacks)


if __name__ == '__main__':
    unittest.main()
