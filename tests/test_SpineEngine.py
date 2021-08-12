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
Unit tests for SpineEngine class.

Inspired from tests for spinetoolbox.ExecutionInstance and spinetoolbox.ResourceMap,
and intended to supersede them.

:author: M. Marin (KTH)
:date:   11.9.2019
"""
import os.path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, NonCallableMagicMock, call, patch

from spine_engine.project_item.connection import Jump, Connection
from spinedb_api import DiffDatabaseMapping, import_scenarios, import_tools
from spinedb_api.filters.scenario_filter import scenario_filter_config
from spinedb_api.filters.tool_filter import tool_filter_config
from spinedb_api.filters.execution_filter import execution_filter_config
from spinedb_api.filters.tools import clear_filter_configs
from spine_engine import ExecutionDirection, SpineEngine, SpineEngineState, ItemExecutionFinishState
from spine_engine.project_item.project_item_resource import ProjectItemResource


def _make_resource(url):
    return ProjectItemResource("name", "database", "label", url)


class TestSpineEngine(unittest.TestCase):
    _LOOP_TWICE = "\n".join(["import sys", "loop_counter = int(sys.argv[1])", "exit(0 if loop_counter < 3 else 1)"])

    @staticmethod
    def _mock_item(
        name, resources_forward=None, resources_backward=None, execute_outcome=ItemExecutionFinishState.SUCCESS
    ):
        """Returns a mock project item.

        Args:
            name (str)
            resources_forward (list): Forward output_resources return value.
            resources_backward (list): Backward output_resources return value.
            execute_outcome (ItemExecutionFinishState): Execution return state.

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
        item.fwd_flt_stacks = []
        item.bwd_flt_stacks = []

        def execute(forward_resources, backward_resources, item=item):
            item.fwd_flt_stacks += [r.metadata.get("filter_stack", ()) for r in forward_resources]
            item.bwd_flt_stacks += [r.metadata.get("filter_stack", ()) for r in backward_resources]
            return execute_outcome

        item.execute.side_effect = execute
        item.exclude_execution = MagicMock()
        for r in resources_forward + resources_backward:
            r.provider_name = item.name
        item.output_resources.side_effect = lambda direction: {
            ExecutionDirection.FORWARD: resources_forward,
            ExecutionDirection.BACKWARD: resources_backward,
        }[direction]
        return item

    def setUp(self):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "mock_project_items"))

    def tearDown(self):
        sys.path.pop(0)

    def test_linear_execution_succeeds(self):
        """Tests execution with three items in a line."""
        url_a_fw = _make_resource("db:///url_a_fw")
        url_b_fw = _make_resource("db:///url_b_fw")
        url_c_fw = _make_resource("db:///url_c_fw")
        url_a_bw = _make_resource("db:///url_a_bw")
        url_b_bw = _make_resource("db:///url_b_bw")
        url_c_bw = _make_resource("db:///url_c_bw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[url_a_bw])
        mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[url_b_bw])
        mock_item_c = self._mock_item("item_c", resources_forward=[url_c_fw], resources_backward=[url_c_bw])
        items = {"item_a": mock_item_a, "item_b": mock_item_b, "item_c": mock_item_c}
        connections = [
            {"from": ("item_a", "right"), "to": ("item_b", "left")},
            {"from": ("item_b", "bottom"), "to": ("item_c", "left")},
        ]
        successors = {"item_a": ["item_b"], "item_b": ["item_c"], "item_c": []}
        execution_permits = {"item_a": True, "item_b": True, "item_c": True}
        engine = SpineEngine(
            items=items,
            connections=connections,
            node_successors=successors,
            execution_permits=execution_permits,
            items_module_name="items_module",
        )
        engine._make_item = lambda name, direction: engine._items[name]
        engine.run()
        item_a_execute_args = [[[], [url_b_bw]]]
        item_b_execute_args = [[[url_a_fw], [url_c_bw]]]
        item_c_execute_calls = [call([url_b_fw], [])]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execute_args)
        mock_item_a.exclude_execution.assert_not_called()
        self._assert_resource_args(mock_item_b.execute.call_args_list, item_b_execute_args)
        mock_item_b.exclude_execution.assert_not_called()
        mock_item_c.execute.assert_has_calls(item_c_execute_calls)
        mock_item_c.exclude_execution.assert_not_called()
        self.assertEqual(engine.state(), SpineEngineState.COMPLETED)

    def test_fork_execution_succeeds(self):
        """Tests execution with three items in a fork."""
        url_a_fw = _make_resource("db:///url_a_fw")
        url_b_fw = _make_resource("db:///url_b_fw")
        url_c_fw = _make_resource("db:///url_c_fw")
        url_a_bw = _make_resource("db:///url_a_bw")
        url_b_bw = _make_resource("db:///url_b_bw")
        url_c_bw = _make_resource("db:///url_c_bw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[url_a_bw])
        mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[url_b_bw])
        mock_item_c = self._mock_item("item_c", resources_forward=[url_c_fw], resources_backward=[url_c_bw])
        items = {"item_a": mock_item_a, "item_b": mock_item_b, "item_c": mock_item_c}
        connections = [
            {"from": ("item_a", "right"), "to": ("item_b", "left")},
            {"from": ("item_b", "bottom"), "to": ("item_c", "left")},
        ]
        successors = {"item_a": ["item_b", "item_c"], "item_b": [], "item_c": []}
        execution_permits = {"item_a": True, "item_b": True, "item_c": True}
        engine = SpineEngine(
            items=items,
            connections=connections,
            node_successors=successors,
            execution_permits=execution_permits,
            items_module_name="items_module",
        )
        engine._make_item = lambda name, direction: engine._items[name]
        engine.run()
        item_a_execute_args = [[[], [url_b_bw, url_c_bw]]]
        item_b_execute_calls = [call([url_a_fw], [])]
        item_c_execute_calls = [call([url_a_fw], [])]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execute_args)
        mock_item_a.exclude_execution.assert_not_called()
        mock_item_b.execute.assert_has_calls(item_b_execute_calls)
        mock_item_b.exclude_execution.assert_not_called()
        mock_item_c.execute.assert_has_calls(item_c_execute_calls)
        mock_item_c.exclude_execution.assert_not_called()
        self.assertEqual(engine.state(), SpineEngineState.COMPLETED)

    def test_execution_permits(self):
        """Tests that the middle item of an item triplet is not executed when its execution permit is False."""
        url_a_fw = _make_resource("db:///url_a_fw")
        url_b_fw = _make_resource("db:///url_b_fw")
        url_c_fw = _make_resource("db:///url_c_fw")
        url_a_bw = _make_resource("db:///url_a_bw")
        url_b_bw = _make_resource("db:///url_b_bw")
        url_c_bw = _make_resource("db:///url_c_bw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[url_a_bw])
        mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[url_b_bw])
        mock_item_c = self._mock_item("item_c", resources_forward=[url_c_fw], resources_backward=[url_c_bw])
        items = {"item_a": mock_item_a, "item_b": mock_item_b, "item_c": mock_item_c}
        connections = [
            {"from": ("item_a", "right"), "to": ("item_b", "left")},
            {"from": ("item_b", "bottom"), "to": ("item_c", "left")},
        ]
        successors = {"item_a": ["item_b"], "item_b": ["item_c"], "item_c": []}
        execution_permits = {"item_a": True, "item_b": False, "item_c": True}
        engine = SpineEngine(
            items=items,
            connections=connections,
            node_successors=successors,
            execution_permits=execution_permits,
            items_module_name="items_module",
        )
        engine._make_item = lambda name, direction: engine._items[name]
        engine.run()
        item_a_execute_args = [[[], [url_b_bw]]]
        item_b_skip_execution_args = [[[url_a_fw], [url_c_bw]]]
        item_c_execute_calls = [call([url_b_fw], [])]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execute_args)
        mock_item_a.exclude_execution.assert_not_called()
        mock_item_b.execute.assert_not_called()
        self._assert_resource_args(mock_item_b.exclude_execution.call_args_list, item_b_skip_execution_args)
        mock_item_b.output_resources.assert_called()
        mock_item_c.execute.assert_has_calls(item_c_execute_calls)
        mock_item_c.exclude_execution.assert_not_called()
        self.assertEqual(engine.state(), SpineEngineState.COMPLETED)

    def test_filter_stacks(self):
        """Tests filter stacks are properly applied."""
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + os.path.join(temp_dir, "db.sqlite")
            db_map = DiffDatabaseMapping(url, create=True)
            import_scenarios(db_map, (("scen1", True), ("scen2", True)))
            import_tools(db_map, ("toolA",))
            db_map.commit_session("Add test data.")
            db_map.connection.close()
            url_a_fw = _make_resource(url)
            url_b_fw = _make_resource("db:///url_b_fw")
            url_c_bw = _make_resource("db:///url_c_bw")
            mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[])
            mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[])
            mock_item_c = self._mock_item("item_c", resources_forward=[], resources_backward=[url_c_bw])
            items = {"item_a": mock_item_a, "item_b": mock_item_b, "item_c": mock_item_c}
            connections = [
                {
                    "from": ("item_a", "right"),
                    "to": ("item_b", "left"),
                    "resource_filters": {url_a_fw.label: {"scenario_filter": [1, 2], "tool_filter": [1]}},
                },
                {"from": ("item_b", "bottom"), "to": ("item_c", "left")},
            ]
            successors = {"item_a": ["item_b"], "item_b": ["item_c"], "item_c": []}
            execution_permits = {"item_a": True, "item_b": True, "item_c": True}
            engine = SpineEngine(
                items=items,
                connections=connections,
                node_successors=successors,
                execution_permits=execution_permits,
                items_module_name="items_module",
            )
            engine._make_item = lambda name, direction: engine._items[name]
            with patch("spine_engine.spine_engine.create_timestamp") as mock_create_timestamp:
                mock_create_timestamp.return_value = "timestamp"
                engine.run()
            # Check that item_b has been executed two times, with the right filters
            self.assertEqual(len(mock_item_b.fwd_flt_stacks), 2)
            self.assertEqual(len(mock_item_b.bwd_flt_stacks), 2)
            self.assertIn((scenario_filter_config("scen1"), tool_filter_config("toolA")), mock_item_b.fwd_flt_stacks)
            self.assertIn((scenario_filter_config("scen2"), tool_filter_config("toolA")), mock_item_b.fwd_flt_stacks)
            self.assertIn(
                (
                    execution_filter_config(
                        {"execution_item": "item_b", "scenarios": ["scen1"], "timestamp": "timestamp"}
                    ),
                ),
                mock_item_b.bwd_flt_stacks,
            )
            self.assertIn(
                (
                    execution_filter_config(
                        {"execution_item": "item_b", "scenarios": ["scen2"], "timestamp": "timestamp"}
                    ),
                ),
                mock_item_b.bwd_flt_stacks,
            )
            # Check that item_c has also been executed two times
            self.assertEqual(len(mock_item_c.fwd_flt_stacks), 2)

    def test_self_jump_succeeds(self):
        mock_item = self._mock_item("item", resources_forward=[], resources_backward=[])
        items = {"item": mock_item}
        connections = []
        jumps = [Jump("item", "bottom", "item", "top", self._LOOP_TWICE).to_dict()]
        successors = {"item": []}
        execution_permits = {"item": True}
        engine = SpineEngine(
            items=items,
            connections=connections,
            jumps=jumps,
            node_successors=successors,
            execution_permits=execution_permits,
            items_module_name="items_module",
        )
        engine._make_item = lambda name, direction: engine._items[name]
        engine.run()
        self.assertEqual(mock_item.execute.call_args_list, 2 * [call([], [])])
        self.assertEqual(engine.state(), SpineEngineState.COMPLETED)

    def test_jump_resources_get_passed_correctly(self):
        resource_fw_a = _make_resource("db:///fw_a")
        resource_bw_a = _make_resource("db:///bw_a")
        item_a = self._mock_item("a", resources_forward=[resource_fw_a], resources_backward=[resource_bw_a])
        resource_fw_b = _make_resource("db:///fw_b")
        resource_bw_b = _make_resource("db:///bw_b")
        item_b = self._mock_item("b", resources_forward=[resource_fw_b], resources_backward=[resource_bw_b])
        resource_fw_c = _make_resource("db:///fw_c")
        resource_bw_c = _make_resource("db:///bw_c")
        item_c = self._mock_item("c", resources_forward=[resource_fw_c], resources_backward=[resource_bw_c])
        resource_fw_d = _make_resource("db:///fw_d")
        resource_bw_d = _make_resource("db:///bw_d")
        item_d = self._mock_item("d", resources_forward=[resource_fw_d], resources_backward=[resource_bw_d])
        items = {"a": item_a, "b": item_b, "c": item_c, "d": item_d}
        connections = [
            c.to_dict()
            for c in (
                Connection("a", "right", "b", "left"),
                Connection("b", "bottom", "c", "top"),
                Connection("c", "left", "d", "right"),
            )
        ]
        jumps = [Jump("c", "right", "b", "right", self._LOOP_TWICE).to_dict()]
        successors = {"a": ["b"], "b": ["c"], "c": ["d"], "d": []}
        execution_permits = {i: True for i in ("a", "b", "c", "d")}
        engine = SpineEngine(
            items=items,
            connections=connections,
            jumps=jumps,
            node_successors=successors,
            execution_permits=execution_permits,
            items_module_name="items_module",
        )
        engine._make_item = lambda name, direction: engine._items[name]
        engine.run()
        self._assert_resource_args(item_a.execute.call_args_list, [[[], [resource_bw_b]]])
        self._assert_resource_args(item_b.execute.call_args_list, 2 * [[[resource_fw_a], [resource_bw_c]]])
        self._assert_resource_args(item_c.execute.call_args_list, 2 * [[[resource_fw_b], [resource_bw_d]]])
        self._assert_resource_args(item_d.execute.call_args_list, [[[resource_fw_c], []]])
        self.assertEqual(engine.state(), SpineEngineState.COMPLETED)

    def test_nested_jump_with_inner_self_jump(self):
        resource_fw_a = _make_resource("db:///fw_a")
        resource_bw_a = _make_resource("db:///bw_a")
        item_a = self._mock_item("a", resources_forward=[resource_fw_a], resources_backward=[resource_bw_a])
        resource_fw_b = _make_resource("db:///fw_b")
        resource_bw_b = _make_resource("db:///bw_b")
        item_b = self._mock_item("b", resources_forward=[resource_fw_b], resources_backward=[resource_bw_b])
        resource_fw_c = _make_resource("db:///fw_c")
        resource_bw_c = _make_resource("db:///bw_c")
        item_c = self._mock_item("c", resources_forward=[resource_fw_c], resources_backward=[resource_bw_c])
        items = {"a": item_a, "b": item_b, "c": item_c}
        connections = [
            c.to_dict() for c in (Connection("a", "right", "b", "left"), Connection("b", "bottom", "c", "top"))
        ]
        jumps = [
            Jump("c", "right", "a", "right", self._LOOP_TWICE).to_dict(),
            Jump("b", "top", "b", "top", self._LOOP_TWICE).to_dict(),
        ]
        successors = {"a": ["b"], "b": ["c"], "c": []}
        execution_permits = {i: True for i in ("a", "b", "c")}
        engine = SpineEngine(
            items=items,
            connections=connections,
            jumps=jumps,
            node_successors=successors,
            execution_permits=execution_permits,
            items_module_name="items_module",
        )
        engine._make_item = lambda name, direction: engine._items[name]
        engine.run()
        expected = 2 * [[[], [resource_bw_b]]]
        self._assert_resource_args(item_a.execute.call_args_list, expected)
        expected = 4 * [[[resource_fw_a], [resource_bw_c]]]
        self._assert_resource_args(item_b.execute.call_args_list, expected)
        expected = 2 * [[[resource_fw_b], []]]
        self._assert_resource_args(item_c.execute.call_args_list, expected)
        self.assertEqual(engine.state(), SpineEngineState.COMPLETED)

    def _assert_resource_args(self, arg_packs, expected_packs):
        self.assertEqual(len(arg_packs), len(expected_packs))
        for pack, expected_pack in zip(arg_packs, expected_packs):
            self.assertEqual(len(pack), len(expected_pack))
            for args, expected in zip(pack[0], expected_pack):
                self.assertEqual(len(args), len(expected))
                for resource, expected_resource in zip(args, expected):
                    self.assertEqual(resource.provider_name, expected_resource.provider_name)
                    self.assertEqual(resource.label, expected_resource.label)
                    self.assertEqual(clear_filter_configs(resource.url), expected_resource.url)
                    self.assertIn("filter_stack", resource.metadata)


if __name__ == '__main__':
    unittest.main()
