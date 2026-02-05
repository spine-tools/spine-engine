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

"""Unit tests for `spine_engine` module."""
from functools import partial
import gc
import os.path
import sys
import unittest
from unittest.mock import MagicMock, NonCallableMagicMock, call, patch
import pytest
from spine_engine import ExecutionDirection, ItemExecutionFinishState, SpineEngine, SpineEngineState
from spine_engine.exception import EngineInitFailed
from spine_engine.project_item.connection import Connection, FilterSettings, Jump
from spine_engine.project_item.project_item_resource import ProjectItemResource, database_resource
from spine_engine.spine_engine import validate_single_jump
from spine_engine.utils.helpers import make_dag
from spinedb_api import DatabaseMapping, append_filter_config, import_scenarios
from spinedb_api.filters.execution_filter import execution_filter_config
from spinedb_api.filters.renamer import entity_class_renamer_config
from spinedb_api.filters.scenario_filter import SCENARIO_FILTER_TYPE, scenario_filter_config
from spinedb_api.filters.tools import clear_filter_configs


@pytest.fixture(autouse=True)
def use_mock_items(monkeypatch):
    mock_items_path = os.path.join(os.path.dirname(__file__), "mock_project_items")
    monkeypatch.syspath_prepend(mock_items_path)


def _make_url_resource(url):
    return ProjectItemResource("name", "database", "label", url, filterable=True)


class TestSpineEngine:
    _LOOP_TWICE = {
        "type": "python-script",
        "script": "\n".join(["import sys", "loop_counter = int(sys.argv[1])", "exit(0 if loop_counter < 2 else 1)"]),
    }

    _LOOP_FOREVER = {
        "type": "python-script",
        "script": "\n".join(["exit(0)"]),
    }

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
            NonCallableMagicMock`
        """
        if resources_forward is None:
            resources_forward = []
        if resources_backward is None:
            resources_backward = []
        item = NonCallableMagicMock()
        item.name = name
        item.short_name = name.lower().replace(" ", "_")
        item.execute.return_value = execute_outcome
        item.exclude_execution = MagicMock()
        for r in resources_forward + resources_backward:
            r.provider_name = item.name
        item.output_resources.side_effect = lambda direction: {
            ExecutionDirection.FORWARD: resources_forward,
            ExecutionDirection.BACKWARD: resources_backward,
        }[direction]
        return item

    @staticmethod
    def _default_forward_url_resource(url, predecessor_name):
        resource = _make_url_resource(url)
        resource.provider_name = predecessor_name
        resource.metadata = {"filter_id": "", "filter_stack": ()}
        return resource

    @staticmethod
    def _default_backward_url_resource(url, item_name, successor_name, scenarios=None):
        resource = _make_url_resource(url)
        resource.provider_name = successor_name
        if scenarios is None:
            scenarios = list()
        resource.metadata = {
            "filter_stack": (
                execution_filter_config(
                    {"execution_item": item_name, "scenarios": scenarios, "timestamp": "timestamp"}
                ),
            )
        }
        return resource

    @staticmethod
    def _create_engine(items, connections, item_instances, execution_permits=None, jumps=None):
        if execution_permits is None:
            execution_permits = {item_name: True for item_name in items}
        with patch("spine_engine.spine_engine.create_timestamp") as mock_create_timestamp:
            mock_create_timestamp.return_value = "timestamp"
            engine = SpineEngine(
                items=items,
                connections=connections,
                jumps=jumps,
                execution_permits=execution_permits,
                items_module_name="items_module",
            )

        def make_item(name, direction):
            if direction == ExecutionDirection.FORWARD:
                return item_instances[name].pop(0)
            return item_instances[name][0]

        engine.make_item = make_item
        return engine

    def _run_engine(self, items, connections, item_instances, execution_permits=None, jumps=None):
        engine = self._create_engine(items, connections, item_instances, execution_permits, jumps)
        engine.run()
        assert engine.state() == SpineEngineState.COMPLETED
        gc.collect()

    def test_single_item_execution(self):
        """Test execution of a single item."""
        url_a_fw = _make_url_resource("db:///url_a_fw")
        url_a_bw = _make_url_resource("db:///url_b_fw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[url_a_bw])
        item_instances = {"item_a": [mock_item_a]}
        items = {"item_a": {"type": "TestItem"}}
        connections = []
        self._run_engine(items, connections, item_instances)
        item_a_execute_args = [[[], []]]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execute_args)
        mock_item_a.exclude_execution.assert_not_called()
        assert mock_item_a.filter_id == ""

    def test_linear_execution(self):
        """Test execution with items a-b-c in a line."""
        url_prefix = "db:///" if sys.platform == "win32" else "db:////"
        url_a_fw = _make_url_resource("db:///url_a_fw")
        url_b_fw = _make_url_resource("db:///url_b_fw")
        url_c_fw = _make_url_resource("db:///url_c_fw")
        url_a_bw = _make_url_resource("db:///url_a_bw")
        url_b_bw = _make_url_resource("db:///url_b_bw")
        url_c_bw = _make_url_resource("db:///url_c_bw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[url_a_bw])
        mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[url_b_bw])
        mock_item_c = self._mock_item("item_c", resources_forward=[url_c_fw], resources_backward=[url_c_bw])
        item_instances = {"item_a": [mock_item_a], "item_b": [mock_item_b], "item_c": [mock_item_c]}
        items = {"item_a": {"type": "TestItem"}, "item_b": {"type": "TestItem"}, "item_c": {"type": "TestItem"}}
        connections = [
            {"from": ("item_a", "right"), "to": ("item_b", "left")},
            {"from": ("item_b", "bottom"), "to": ("item_c", "left")},
        ]
        self._run_engine(items, connections, item_instances)
        expected_bw_resource = self._default_backward_url_resource(url_prefix + "url_b_bw", "item_a", "item_b")
        item_a_execute_args = [[[], [expected_bw_resource]]]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execute_args)
        assert mock_item_a.filter_id == ""
        mock_item_a.exclude_execution.assert_not_called()
        expected_fw_resource = self._default_forward_url_resource("db:///url_a_fw", "item_a")
        expected_bw_resource = self._default_backward_url_resource(url_prefix + "url_c_bw", "item_b", "item_c")
        item_b_execute_args = [[[expected_fw_resource], [expected_bw_resource]]]
        self._assert_resource_args(mock_item_b.execute.call_args_list, item_b_execute_args)
        assert mock_item_b.filter_id == ""
        mock_item_b.exclude_execution.assert_not_called()
        expected_fw_resource = self._default_forward_url_resource("db:///url_b_fw", "item_b")
        item_c_execute_args = [[[expected_fw_resource], []]]
        self._assert_resource_args(mock_item_c.execute.call_args_list, item_c_execute_args)
        mock_item_c.exclude_execution.assert_not_called()
        assert mock_item_c.filter_id == ""

    def test_fork_execution(self):
        """Test execution that forks from item a to items b and c."""
        url_a_fw = _make_url_resource("db:///url_a_fw")
        url_b_fw = _make_url_resource("db:///url_b_fw")
        url_c_fw = _make_url_resource("db:///url_c_fw")
        url_a_bw = _make_url_resource("db:///url_a_bw")
        url_b_bw = _make_url_resource("db:///url_b_bw")
        url_c_bw = _make_url_resource("db:///url_c_bw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[url_a_bw])
        mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[url_b_bw])
        mock_item_c = self._mock_item("item_c", resources_forward=[url_c_fw], resources_backward=[url_c_bw])
        item_instances = {"item_a": [mock_item_a], "item_b": [mock_item_b], "item_c": [mock_item_c]}
        items = {"item_a": {"type": "TestItem"}, "item_b": {"type": "TestItem"}, "item_c": {"type": "TestItem"}}
        connections = [
            {"from": ("item_a", "right"), "to": ("item_b", "left")},
            {"from": ("item_a", "bottom"), "to": ("item_c", "left")},
        ]
        url_prefix = "db:///" if sys.platform == "win32" else "db:////"
        self._run_engine(items, connections, item_instances)
        expected_bw_resource1 = self._default_backward_url_resource(url_prefix + "url_b_bw", "item_a", "item_b")
        expected_bw_resource2 = self._default_backward_url_resource(url_prefix + "url_c_bw", "item_a", "item_c")
        item_a_execute_args = [[[], [expected_bw_resource1, expected_bw_resource2]]]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execute_args)
        assert mock_item_a.filter_id == ""
        mock_item_a.exclude_execution.assert_not_called()
        expected_fw_resource = self._default_forward_url_resource("db:///url_a_fw", "item_a")
        item_b_execute_calls = [[[expected_fw_resource], []]]
        self._assert_resource_args(mock_item_b.execute.call_args_list, item_b_execute_calls)
        mock_item_b.exclude_execution.assert_not_called()
        assert mock_item_b.filter_id == ""
        item_c_execute_calls = [[[expected_fw_resource], []]]
        self._assert_resource_args(mock_item_c.execute.call_args_list, item_c_execute_calls)
        mock_item_c.exclude_execution.assert_not_called()
        assert mock_item_c.filter_id == ""

    def test_branch_merge_execution(self):
        """Tests execution with items a and b as direct successors for c."""
        url_a_fw = _make_url_resource("db:///url_a_fw")
        url_b_fw = _make_url_resource("db:///url_b_fw")
        url_c_fw = _make_url_resource("db:///url_c_fw")
        url_a_bw = _make_url_resource("db:///url_a_bw")
        url_b_bw = _make_url_resource("db:///url_b_bw")
        url_c_bw = _make_url_resource("db:///url_c_bw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[url_a_bw])
        mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[url_b_bw])
        mock_item_c = self._mock_item("item_c", resources_forward=[url_c_fw], resources_backward=[url_c_bw])
        item_instances = {"item_a": [mock_item_a], "item_b": [mock_item_b], "item_c": [mock_item_c]}
        items = {"item_a": {"type": "TestItem"}, "item_b": {"type": "TestItem"}, "item_c": {"type": "TestItem"}}
        connections = [
            {"from": ("item_a", "right"), "to": ("item_c", "left")},
            {"from": ("item_b", "bottom"), "to": ("item_c", "left")},
        ]
        self._run_engine(items, connections, item_instances)
        url_prefix = "db:///" if sys.platform == "win32" else "db:////"
        expected_bw_resource = self._default_backward_url_resource(url_prefix + "url_c_bw", "item_a", "item_c")
        item_a_execute_args = [[[], [expected_bw_resource]]]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execute_args)
        assert mock_item_a.filter_id == ""
        expected_bw_resource = self._default_backward_url_resource(url_prefix + "url_c_bw", "item_b", "item_c")
        item_b_execute_calls = [[[], [expected_bw_resource]]]
        self._assert_resource_args(mock_item_b.execute.call_args_list, item_b_execute_calls)
        assert mock_item_b.filter_id == ""
        expected_fw_resource1 = self._default_forward_url_resource("db:///url_a_fw", "item_a")
        expected_fw_resource2 = self._default_forward_url_resource("db:///url_b_fw", "item_b")
        item_c_execute_calls = [[[expected_fw_resource1, expected_fw_resource2], []]]
        self._assert_resource_args(mock_item_c.execute.call_args_list, item_c_execute_calls)
        assert mock_item_c.filter_id == ""

    def test_execution_permits(self):
        """Tests that the middle item of an item triplet is not executed when its execution permit is False."""
        url_a_fw = _make_url_resource("db:///url_a_fw")
        url_b_fw = _make_url_resource("db:///url_b_fw")
        url_c_fw = _make_url_resource("db:///url_c_fw")
        url_a_bw = _make_url_resource("db:///url_a_bw")
        url_b_bw = _make_url_resource("db:///url_b_bw")
        url_c_bw = _make_url_resource("db:///url_c_bw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[url_a_bw])
        mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[url_b_bw])
        mock_item_c = self._mock_item("item_c", resources_forward=[url_c_fw], resources_backward=[url_c_bw])
        item_instances = {"item_a": [mock_item_a], "item_b": [mock_item_b], "item_c": [mock_item_c]}
        items = {"item_a": {"type": "TestItem"}, "item_b": {"type": "TestItem"}, "item_c": {"type": "TestItem"}}
        connections = [
            {"from": ("item_a", "right"), "to": ("item_b", "left")},
            {"from": ("item_b", "bottom"), "to": ("item_c", "left")},
        ]
        execution_permits = {"item_a": True, "item_b": False, "item_c": True}
        self._run_engine(items, connections, item_instances, execution_permits=execution_permits)
        url_prefix = "db:///" if sys.platform == "win32" else "db:////"
        expected_bw_resource = self._default_backward_url_resource(url_prefix + "url_b_bw", "item_a", "item_b")
        item_a_execute_args = [[[], [expected_bw_resource]]]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execute_args)
        mock_item_a.exclude_execution.assert_not_called()
        mock_item_b.execute.assert_not_called()
        expected_fw_resource = self._default_forward_url_resource("db:///url_a_fw", "item_a")
        expected_bw_resource = self._default_backward_url_resource(url_prefix + "url_c_bw", "item_b", "item_c")
        item_b_skip_execution_args = [[[expected_fw_resource], [expected_bw_resource]]]
        self._assert_resource_args(mock_item_b.exclude_execution.call_args_list, item_b_skip_execution_args)
        mock_item_b.output_resources.assert_called()
        expected_fw_resource = self._default_forward_url_resource("db:///url_b_fw", "item_b")
        item_c_execute_calls = [[[expected_fw_resource], []]]
        self._assert_resource_args(mock_item_c.execute.call_args_list, item_c_execute_calls)
        mock_item_c.exclude_execution.assert_not_called()

    def test_filter_stacks(self, tmp_path):
        """Tests filter stacks are properly applied."""
        url = "sqlite:///" + str(tmp_path / "db.sqlite")
        with DatabaseMapping(url, create=True) as db_map:
            import_scenarios(db_map, (("scen1", True), ("scen2", True)))
            db_map.commit_session("Add test data.")
        db_map.close()
        url_prefix = "db:///" if sys.platform == "win32" else "db:////"
        url_a_fw = _make_url_resource(url)
        url_b_fw1 = _make_url_resource("db:///url_b_fw")
        url_b_fw2 = _make_url_resource("db:///url_b_fw")
        url_c_bw = _make_url_resource("db:///url_c_bw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[])
        mock_item_b1 = self._mock_item("item_b", resources_forward=[url_b_fw1], resources_backward=[])
        mock_item_b2 = self._mock_item("item_b", resources_forward=[url_b_fw2], resources_backward=[])
        mock_item_c1 = self._mock_item("item_c", resources_forward=[], resources_backward=[url_c_bw])
        mock_item_c2 = self._mock_item("item_c", resources_forward=[], resources_backward=[url_c_bw])
        item_instances = {
            "item_a": [mock_item_a],
            "item_b": [mock_item_b1, mock_item_b2],
            "item_c": [mock_item_c1, mock_item_c2],
        }
        items = {
            "item_a": {"type": "TestItem"},
            "item_b": {"type": "TestItem"},
            "item_c": {"type": "TestItem"},
        }
        connections = [
            {
                "from": ("item_a", "right"),
                "to": ("item_b", "left"),
                "filter_settings": FilterSettings(
                    {url_a_fw.label: {"scenario_filter": {"scen1": True, "scen2": True}}}
                ).to_dict(),
            },
            {"from": ("item_b", "bottom"), "to": ("item_c", "left")},
        ]
        self._run_engine(items, connections, item_instances)
        item_a_execution_args = [[[], []]]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execution_args)
        assert mock_item_a.filter_id == ""
        # Check that item_b has been executed two times, with the right filters
        expected_fw_resource1 = ProjectItemResource("item_a", "database", "label", url)
        expected_filter_stack1 = (scenario_filter_config("scen1"),)
        expected_fw_resource1.metadata = {"filter_stack": expected_filter_stack1, "filter_id": ""}
        expected_bw_resource1 = self._default_backward_url_resource(
            url_prefix + "url_c_bw", "item_b", "item_c", ["scen1"]
        )
        item_b_execution_args = [[[expected_fw_resource1], [expected_bw_resource1]]]
        self._assert_resource_args(mock_item_b1.execute.call_args_list, item_b_execution_args)
        assert mock_item_b1.filter_id == "scen1 - item_a"
        expected_fw_resource2 = ProjectItemResource("item_a", "database", "label", url)
        expected_filter_stack2 = (scenario_filter_config("scen2"),)
        expected_fw_resource2.metadata = {"filter_stack": expected_filter_stack2, "filter_id": ""}
        expected_bw_resource2 = self._default_backward_url_resource(
            url_prefix + "url_c_bw", "item_b", "item_c", ["scen2"]
        )
        item_b_execution_args = [[[expected_fw_resource2], [expected_bw_resource2]]]
        self._assert_resource_args(mock_item_b2.execute.call_args_list, item_b_execution_args)
        assert mock_item_b2.filter_id == "scen2 - item_a"
        # Check that item_c has been executed twice, with the right filters
        expected_fw_resource1 = ProjectItemResource("item_b", "database", "label", "db:///url_b_fw")
        expected_fw_resource1.metadata = {
            "filter_stack": expected_filter_stack1,
            "filter_id": "scen1 - item_a",
        }
        item_c_execution_args = [[[expected_fw_resource1], []]]
        self._assert_resource_args(mock_item_c1.execute.call_args_list, item_c_execution_args)
        assert mock_item_c1.filter_id == "scen1 - item_b"
        expected_fw_resource2 = ProjectItemResource("item_b", "database", "label", "db:///url_b_fw")
        expected_fw_resource2.metadata = {
            "filter_stack": expected_filter_stack2,
            "filter_id": "scen2 - item_a",
        }
        item_c_execution_args = [[[expected_fw_resource2], []]]
        self._assert_resource_args(mock_item_c2.execute.call_args_list, item_c_execution_args)
        assert mock_item_c2.filter_id == "scen2 - item_b"

    def test_parallel_execution_ends_when_no_output_resources_are_generated(self, tmp_path):
        url = "sqlite:///" + str(tmp_path / "db.sqlite")
        with DatabaseMapping(url, create=True) as db_map:
            import_scenarios(db_map, (("scen1", True), ("scen2", True)))
            db_map.commit_session("Add test data.")
        db_map.close()
        gc.collect()
        url_prefix = "db:///" if sys.platform == "win32" else "db:////"
        url_a_fw = _make_url_resource(url)
        url_c_bw = _make_url_resource("db:///url_c_bw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[])
        mock_item_b1 = self._mock_item("item_b", resources_forward=[], resources_backward=[])
        mock_item_b2 = self._mock_item("item_b", resources_forward=[], resources_backward=[])
        mock_item_c = self._mock_item("item_c", resources_forward=[], resources_backward=[url_c_bw])
        item_instances = {"item_a": [mock_item_a], "item_b": [mock_item_b1, mock_item_b2], "item_c": [mock_item_c]}
        items = {
            "item_a": {"type": "TestItem"},
            "item_b": {"type": "TestItem"},
            "item_c": {"type": "TestItem"},
        }
        connections = [
            {
                "from": ("item_a", "right"),
                "to": ("item_b", "left"),
                "filter_settings": FilterSettings(
                    {url_a_fw.label: {"scenario_filter": {"scen1": True, "scen2": True}}}
                ).to_dict(),
            },
            {"from": ("item_b", "bottom"), "to": ("item_c", "left")},
        ]
        self._run_engine(items, connections, item_instances)
        item_a_execution_args = [[[], []]]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execution_args)
        assert mock_item_a.filter_id == ""
        # Check that item_b has been executed two times, with the right filters
        expected_fw_resource1 = ProjectItemResource("item_a", "database", "label", url)
        expected_filter_stack1 = (scenario_filter_config("scen1"),)
        expected_fw_resource1.metadata = {"filter_stack": expected_filter_stack1, "filter_id": ""}
        expected_bw_resource1 = self._default_backward_url_resource(
            url_prefix + "url_c_bw", "item_b", "item_c", ["scen1"]
        )
        item_b_execution_args = [[[expected_fw_resource1], [expected_bw_resource1]]]
        self._assert_resource_args(mock_item_b1.execute.call_args_list, item_b_execution_args)
        assert mock_item_b1.filter_id == "scen1 - item_a"
        expected_fw_resource2 = ProjectItemResource("item_a", "database", "label", url)
        expected_filter_stack2 = (scenario_filter_config("scen2"),)
        expected_fw_resource2.metadata = {"filter_stack": expected_filter_stack2, "filter_id": ""}
        expected_bw_resource2 = self._default_backward_url_resource(
            url_prefix + "url_c_bw", "item_b", "item_c", ["scen2"]
        )
        item_b_execution_args = [[[expected_fw_resource2], [expected_bw_resource2]]]
        self._assert_resource_args(mock_item_b2.execute.call_args_list, item_b_execution_args)
        assert mock_item_b2.filter_id == "scen2 - item_a"
        # Check that item_c has been executed only once
        self._assert_resource_args(mock_item_c.execute.call_args_list, [[[], []]])
        assert mock_item_c.filter_id == ""

    def test_filter_stacks_and_multiple_file_output_resources(self, tmp_path):
        """Multiple file output resources should be combined correctly for a successor"""
        url = "sqlite:///" + str(tmp_path / "db.sqlite")
        with DatabaseMapping(url, create=True) as db_map:
            import_scenarios(db_map, (("scen1", True), ("scen2", True)))
            db_map.commit_session("Add test data.")
        db_map.close()
        gc.collect()
        url_prefix = "db:///" if sys.platform == "win32" else "db:////"
        url_a_fw = _make_url_resource(url)
        file_b_fw_11 = ProjectItemResource("item_b", "file", "label_1")
        file_b_fw_12 = ProjectItemResource("item_b", "file", "label_1")
        file_b_fw_21 = ProjectItemResource("item_b", "file", "label_2")
        file_b_fw_22 = ProjectItemResource("item_b", "file", "label_2")
        url_c_bw = _make_url_resource("db:///url_c_bw")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[])
        mock_item_b1 = self._mock_item("item_b", resources_forward=[file_b_fw_11, file_b_fw_21], resources_backward=[])
        mock_item_b2 = self._mock_item("item_b", resources_forward=[file_b_fw_12, file_b_fw_22], resources_backward=[])
        mock_item_c1 = self._mock_item("item_c", resources_forward=[], resources_backward=[url_c_bw])
        mock_item_c2 = self._mock_item("item_c", resources_forward=[], resources_backward=[url_c_bw])
        item_instances = {
            "item_a": [mock_item_a],
            "item_b": [mock_item_b1, mock_item_b2],
            "item_c": [mock_item_c1, mock_item_c2],
        }
        items = {
            "item_a": {"type": "TestItem"},
            "item_b": {"type": "TestItem"},
            "item_c": {"type": "TestItem"},
        }
        connections = [
            {
                "from": ("item_a", "right"),
                "to": ("item_b", "left"),
                "filter_settings": FilterSettings(
                    {url_a_fw.label: {"scenario_filter": {"scen1": True, "scen2": True}}}
                ).to_dict(),
            },
            {"from": ("item_b", "bottom"), "to": ("item_c", "left")},
        ]
        self._run_engine(items, connections, item_instances)
        item_a_execution_args = [[[], []]]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execution_args)
        assert mock_item_a.filter_id == ""
        # Check that item_b has been executed two times, with the right filters
        expected_fw_resource1 = ProjectItemResource("item_a", "database", "label", url)
        expected_filter_stack1 = (scenario_filter_config("scen1"),)
        expected_fw_resource1.metadata = {"filter_stack": expected_filter_stack1, "filter_id": ""}
        expected_bw_resource1 = self._default_backward_url_resource(
            url_prefix + "url_c_bw", "item_b", "item_c", ["scen1"]
        )
        item_b_execution_args = [[[expected_fw_resource1], [expected_bw_resource1]]]
        self._assert_resource_args(mock_item_b1.execute.call_args_list, item_b_execution_args)
        assert mock_item_b1.filter_id == "scen1 - item_a"
        expected_fw_resource2 = ProjectItemResource("item_a", "database", "label", url)
        expected_filter_stack2 = (scenario_filter_config("scen2"),)
        expected_fw_resource2.metadata = {"filter_stack": expected_filter_stack2, "filter_id": ""}
        expected_bw_resource2 = self._default_backward_url_resource(
            url_prefix + "url_c_bw", "item_b", "item_c", ["scen2"]
        )
        item_b_execution_args = [[[expected_fw_resource2], [expected_bw_resource2]]]
        self._assert_resource_args(mock_item_b2.execute.call_args_list, item_b_execution_args)
        assert mock_item_b2.filter_id == "scen2 - item_a"
        # Check that item_c has been executed twice, with the right filters
        expected_fw_resource1 = ProjectItemResource("item_b", "file", "label_1")
        expected_fw_resource1.metadata = {
            "filter_stack": expected_filter_stack1,
            "filter_id": "scen1 - item_a",
        }
        expected_fw_resource2 = ProjectItemResource("item_b", "file", "label_2")
        expected_fw_resource2.metadata = {
            "filter_stack": expected_filter_stack1,
            "filter_id": "scen1 - item_a",
        }
        item_c_execution_args = [[[expected_fw_resource1, expected_fw_resource2], []]]
        self._assert_resource_args(mock_item_c1.execute.call_args_list, item_c_execution_args)
        assert mock_item_c1.filter_id == "scen1 - item_a"
        expected_fw_resource3 = ProjectItemResource("item_b", "file", "label_1")
        expected_fw_resource3.metadata = {
            "filter_stack": expected_filter_stack2,
            "filter_id": "scen2 - item_a",
        }
        expected_fw_resource4 = ProjectItemResource("item_b", "file", "label_2")
        expected_fw_resource4.metadata = {
            "filter_stack": expected_filter_stack2,
            "filter_id": "scen2 - item_a",
        }
        item_c_execution_args = [[[expected_fw_resource3, expected_fw_resource4], []]]
        self._assert_resource_args(mock_item_c2.execute.call_args_list, item_c_execution_args)
        assert mock_item_c2.filter_id == "scen2 - item_a"

    def test_merge_two_filtered_database_branches(self, tmp_path):
        urlA = "sqlite:///" + str(tmp_path / "dbA.sqlite")
        with DatabaseMapping(urlA, create=True) as db_map:
            import_scenarios(db_map, (("scenA1", True), ("scenA2", True)))
            db_map.commit_session("Add test data.")
        db_map.close()
        urlB = "sqlite:///" + str(tmp_path / "dbB.sqlite")
        with DatabaseMapping(urlB, create=True) as db_map:
            import_scenarios(db_map, (("scenB1", True), ("scenB2", True)))
            db_map.commit_session("Add test data.")
        db_map.close()
        gc.collect()
        url_a_fw = _make_url_resource(urlA)
        url_b_fw = _make_url_resource(urlB)
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[])
        mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[])
        mock_item_c1 = self._mock_item("item_c", resources_forward=[], resources_backward=[])
        mock_item_c2 = self._mock_item("item_c", resources_forward=[], resources_backward=[])
        mock_item_c3 = self._mock_item("item_c", resources_forward=[], resources_backward=[])
        mock_item_c4 = self._mock_item("item_c", resources_forward=[], resources_backward=[])
        item_instances = {
            "item_a": [mock_item_a],
            "item_b": [mock_item_b],
            "item_c": [mock_item_c1, mock_item_c2, mock_item_c3, mock_item_c4],
        }
        items = {
            "item_a": {"type": "TestItem"},
            "item_b": {"type": "TestItem"},
            "item_c": {"type": "TestItem"},
        }
        connections = [
            {
                "from": ("item_a", "right"),
                "to": ("item_c", "left"),
                "filter_settings": FilterSettings(
                    {url_a_fw.label: {"scenario_filter": {"scenA1": True, "scenA2": True}}}
                ).to_dict(),
            },
            {
                "from": ("item_b", "right"),
                "to": ("item_c", "left"),
                "filter_settings": FilterSettings(
                    {url_b_fw.label: {"scenario_filter": {"scenB1": True, "scenB2": True}}}
                ).to_dict(),
            },
        ]
        self._run_engine(items, connections, item_instances)
        item_a_execution_args = [[[], []]]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execution_args)
        item_b_execution_args = [[[], []]]
        self._assert_resource_args(mock_item_b.execute.call_args_list, item_b_execution_args)
        # Check that item_c has been executed four times, with all combinations of item_a and item_b filters
        expected_fw_resource1 = ProjectItemResource("item_a", "database", "label", urlA)
        expected_filter_stack1 = (scenario_filter_config("scenA1"),)
        expected_fw_resource1.metadata = {"filter_stack": expected_filter_stack1, "filter_id": ""}
        expected_fw_resource2 = ProjectItemResource("item_b", "database", "label", urlB)
        expected_filter_stack2 = (scenario_filter_config("scenB1"),)
        expected_fw_resource2.metadata = {"filter_stack": expected_filter_stack2, "filter_id": ""}
        item_c_execution_args = [[[expected_fw_resource1, expected_fw_resource2], []]]
        self._assert_resource_args(mock_item_c1.execute.call_args_list, item_c_execution_args)
        expected_fw_resource3 = ProjectItemResource("item_b", "database", "label", urlB)
        expected_filter_stack3 = (scenario_filter_config("scenB2"),)
        expected_fw_resource3.metadata = {"filter_stack": expected_filter_stack3, "filter_id": ""}
        item_c_execution_args = [[[expected_fw_resource1, expected_fw_resource3], []]]
        self._assert_resource_args(mock_item_c2.execute.call_args_list, item_c_execution_args)
        expected_fw_resource4 = ProjectItemResource("item_a", "database", "label", urlA)
        expected_filter_stack4 = (scenario_filter_config("scenA2"),)
        expected_fw_resource4.metadata = {"filter_stack": expected_filter_stack4, "filter_id": ""}
        item_c_execution_args = [[[expected_fw_resource4, expected_fw_resource2], []]]
        self._assert_resource_args(mock_item_c3.execute.call_args_list, item_c_execution_args)
        item_c_execution_args = [[[expected_fw_resource4, expected_fw_resource3], []]]
        self._assert_resource_args(mock_item_c4.execute.call_args_list, item_c_execution_args)

    def test_merge_two_filtered_file_resource_branches(self, tmp_path):
        urlA = "sqlite:///" + str(tmp_path / "dbA.sqlite")
        with DatabaseMapping(urlA, create=True) as db_map:
            import_scenarios(db_map, (("scenA1", True), ("scenA2", True)))
            db_map.commit_session("Add test data.")
        db_map.close()
        urlB = "sqlite:///" + str(tmp_path / "dbB.sqlite")
        with DatabaseMapping(urlB, create=True) as db_map:
            import_scenarios(db_map, (("scenB1", True), ("scenB2", True)))
            db_map.commit_session("Add test data.")
        db_map.close()
        gc.collect()
        url_a_fw = _make_url_resource(urlA)
        url_b_fw = _make_url_resource(urlB)
        file_c_fw_11 = ProjectItemResource("item_c", "file", "label_1")
        file_c_fw_12 = ProjectItemResource("item_c", "file", "label_1")
        file_c_fw_21 = ProjectItemResource("item_c", "file", "label_2")
        file_c_fw_22 = ProjectItemResource("item_c", "file", "label_2")
        file_d_fw_11 = ProjectItemResource("item_d", "file", "label_3")
        file_d_fw_12 = ProjectItemResource("item_d", "file", "label_3")
        file_d_fw_21 = ProjectItemResource("item_d", "file", "label_4")
        file_d_fw_22 = ProjectItemResource("item_d", "file", "label_4")
        mock_item_a = self._mock_item("item_a", resources_forward=[url_a_fw], resources_backward=[])
        mock_item_b = self._mock_item("item_b", resources_forward=[url_b_fw], resources_backward=[])
        mock_item_c1 = self._mock_item("item_c", resources_forward=[file_c_fw_11, file_c_fw_21], resources_backward=[])
        mock_item_c2 = self._mock_item("item_c", resources_forward=[file_c_fw_12, file_c_fw_22], resources_backward=[])
        mock_item_d1 = self._mock_item("item_d", resources_forward=[file_d_fw_11, file_d_fw_21], resources_backward=[])
        mock_item_d2 = self._mock_item("item_d", resources_forward=[file_d_fw_12, file_d_fw_22], resources_backward=[])
        mock_item_e1 = self._mock_item("item_e", resources_forward=[], resources_backward=[])
        mock_item_e2 = self._mock_item("item_e", resources_forward=[], resources_backward=[])
        mock_item_e3 = self._mock_item("item_e", resources_forward=[], resources_backward=[])
        mock_item_e4 = self._mock_item("item_e", resources_forward=[], resources_backward=[])
        item_instances = {
            "item_a": [mock_item_a],
            "item_b": [mock_item_b],
            "item_c": [mock_item_c1, mock_item_c2],
            "item_d": [mock_item_d1, mock_item_d2],
            "item_e": [mock_item_e1, mock_item_e2, mock_item_e3, mock_item_e4],
        }
        items = {
            "item_a": {"type": "TestItem"},
            "item_b": {"type": "TestItem"},
            "item_c": {"type": "TestItem"},
            "item_d": {"type": "TestItem"},
            "item_e": {"type": "TestItem"},
        }
        connections = [
            {
                "from": ("item_a", "right"),
                "to": ("item_c", "left"),
                "filter_settings": FilterSettings(
                    {url_a_fw.label: {"scenario_filter": {"scenA1": True, "scenA2": True}}}
                ).to_dict(),
            },
            {
                "from": ("item_b", "right"),
                "to": ("item_d", "left"),
                "filter_settings": FilterSettings(
                    {url_b_fw.label: {"scenario_filter": {"scenB1": True, "scenB2": True}}}
                ).to_dict(),
            },
            {"from": ("item_c", "right"), "to": ("item_e", "left")},
            {"from": ("item_d", "bottom"), "to": ("item_e", "top")},
        ]
        self._run_engine(items, connections, item_instances)
        item_a_execution_args = [[[], []]]
        self._assert_resource_args(mock_item_a.execute.call_args_list, item_a_execution_args)
        assert mock_item_a.filter_id == ""
        item_b_execution_args = [[[], []]]
        self._assert_resource_args(mock_item_b.execute.call_args_list, item_b_execution_args)
        assert mock_item_b.filter_id == ""
        # Check that item_c has been executed twice, with all combinations of item_a's filters
        expected_fw_resource1 = ProjectItemResource("item_a", "database", "label", urlA)
        expected_filter_stack1 = (scenario_filter_config("scenA1"),)
        expected_fw_resource1.metadata = {"filter_stack": expected_filter_stack1, "filter_id": ""}
        item_c_execution_args = [[[expected_fw_resource1], []]]
        self._assert_resource_args(mock_item_c1.execute.call_args_list, item_c_execution_args)
        assert mock_item_c1.filter_id == "scenA1 - item_a"
        expected_fw_resource2 = ProjectItemResource("item_a", "database", "label", urlA)
        expected_filter_stack2 = (scenario_filter_config("scenA2"),)
        expected_fw_resource2.metadata = {"filter_stack": expected_filter_stack2, "filter_id": ""}
        item_c_execution_args = [[[expected_fw_resource2], []]]
        self._assert_resource_args(mock_item_c2.execute.call_args_list, item_c_execution_args)
        assert mock_item_c2.filter_id == "scenA2 - item_a"
        # Check that item_d has been executed twice, with all combinations of item_b's filters
        expected_fw_resource3 = ProjectItemResource("item_b", "database", "label", urlB)
        expected_filter_stack3 = (scenario_filter_config("scenB1"),)
        expected_fw_resource3.metadata = {"filter_stack": expected_filter_stack3, "filter_id": ""}
        item_d_execution_args = [[[expected_fw_resource3], []]]
        self._assert_resource_args(mock_item_d1.execute.call_args_list, item_d_execution_args)
        assert mock_item_d1.filter_id == "scenB1 - item_b"
        expected_fw_resource4 = ProjectItemResource("item_b", "database", "label", urlB)
        expected_filter_stack4 = (scenario_filter_config("scenB2"),)
        expected_fw_resource4.metadata = {"filter_stack": expected_filter_stack4, "filter_id": ""}
        item_d_execution_args = [[[expected_fw_resource4], []]]
        self._assert_resource_args(mock_item_d2.execute.call_args_list, item_d_execution_args)
        assert mock_item_d2.filter_id == "scenB2 - item_b"
        # Check that item_e has been executed four times, with all combinations of item_c's and item_d's resources.
        expected_fw_resource5 = ProjectItemResource("item_c", "file", "label_1")
        expected_filter_stack5 = (scenario_filter_config("scenA1"),)
        expected_fw_resource5.metadata = {
            "filter_stack": expected_filter_stack5,
            "filter_id": "scenA1 - item_a",
        }
        expected_fw_resource6 = ProjectItemResource("item_c", "file", "label_2")
        expected_fw_resource6.metadata = {
            "filter_stack": expected_filter_stack5,
            "filter_id": "scenA1 - item_a",
        }
        expected_fw_resource7 = ProjectItemResource("item_d", "file", "label_3")
        expected_filter_stack7 = (scenario_filter_config("scenB1"),)
        expected_fw_resource7.metadata = {
            "filter_stack": expected_filter_stack7,
            "filter_id": "scenB1 - item_b",
        }
        expected_fw_resource8 = ProjectItemResource("item_d", "file", "label_4")
        expected_fw_resource8.metadata = {
            "filter_stack": expected_filter_stack7,
            "filter_id": "scenB1 - item_b",
        }
        item_e_execution_args = [
            [[expected_fw_resource5, expected_fw_resource6, expected_fw_resource7, expected_fw_resource8], []]
        ]
        self._assert_resource_args(mock_item_e1.execute.call_args_list, item_e_execution_args)
        assert mock_item_e1.filter_id == "scenA1 - item_a & scenB1 - item_b"
        expected_fw_resource9 = ProjectItemResource("item_d", "file", "label_3")
        expected_filter_stack9 = (scenario_filter_config("scenB2"),)
        expected_fw_resource9.metadata = {
            "filter_stack": expected_filter_stack9,
            "filter_id": "scenB2 - item_b",
        }
        expected_fw_resource10 = ProjectItemResource("item_d", "file", "label_4")
        expected_fw_resource10.metadata = {
            "filter_stack": expected_filter_stack9,
            "filter_id": "scenB2 - item_b",
        }
        item_e_execution_args = [
            [[expected_fw_resource5, expected_fw_resource6, expected_fw_resource9, expected_fw_resource10], []]
        ]
        self._assert_resource_args(mock_item_e2.execute.call_args_list, item_e_execution_args)
        assert mock_item_e2.filter_id == "scenA1 - item_a & scenB2 - item_b"
        expected_fw_resource11 = ProjectItemResource("item_c", "file", "label_1")
        expected_filter_stack11 = (scenario_filter_config("scenA2"),)
        expected_fw_resource11.metadata = {
            "filter_stack": expected_filter_stack11,
            "filter_id": "scenA2 - item_a",
        }
        expected_fw_resource12 = ProjectItemResource("item_c", "file", "label_2")
        expected_fw_resource12.metadata = {
            "filter_stack": expected_filter_stack11,
            "filter_id": "scenA2 - item_a",
        }
        item_e_execution_args = [
            [[expected_fw_resource11, expected_fw_resource12, expected_fw_resource7, expected_fw_resource8], []]
        ]
        self._assert_resource_args(mock_item_e3.execute.call_args_list, item_e_execution_args)
        assert mock_item_e3.filter_id == "scenA2 - item_a & scenB1 - item_b"
        item_e_execution_args = [
            [[expected_fw_resource11, expected_fw_resource12, expected_fw_resource9, expected_fw_resource10], []]
        ]
        self._assert_resource_args(mock_item_e4.execute.call_args_list, item_e_execution_args)
        assert mock_item_e4.filter_id == "scenA2 - item_a & scenB2 - item_b"

    def test_same_filtered_resources_get_merged(self, tmp_path):
        """Tests this type of workflow:

        DS -> DT -> Tool1 -> Tool2
               |               |
               +---------------+
        Tool2 is expected to get DB resources from DT and file resources from Tool1 under a single filter.
        """
        url = "sqlite:///" + str(tmp_path / "db.sqlite")
        with DatabaseMapping(url, create=True) as db_map:
            db_map.add_scenario(name="scenario1")
            db_map.commit_session("Add test data.")
        db_map.close()
        url_fw = _make_url_resource(url)
        mock_ds = self._mock_item("DS", resources_forward=[url_fw], resources_backward=[])
        transformed_url = append_filter_config(url, scenario_filter_config("scenario1"))
        transformed_url = append_filter_config(transformed_url, entity_class_renamer_config(unit="not_node"))
        transformed_url_fw = database_resource("DT", transformed_url, "<transformed url>@DT")
        mock_dt = self._mock_item("DT", resources_forward=[transformed_url_fw])
        file_resource = ProjectItemResource("Tool1", "file", "label_1")
        mock_tool1 = self._mock_item("Tool1", resources_forward=[file_resource])
        mock_tool2 = self._mock_item("Tool2")
        item_instances = {
            "DS": [mock_ds],
            "DT": [mock_dt],
            "Tool1": [mock_tool1],
            "Tool2": [mock_tool2],
        }
        items = {
            "DS": {"type": "TestItem"},
            "DT": {"type": "TestItem"},
            "Tool1": {"type": "TestItem"},
            "Tool2": {"type": "TestItem"},
        }
        connections = [
            {
                "from": ("DS", "left"),
                "to": ("DT", "right"),
                "filter_settings": FilterSettings({url_fw.label: {"scenario_filter": {"scenario1": True}}}).to_dict(),
            },
            {
                "from": ("DT", "left"),
                "to": ("Tool1", "right"),
            },
            {"from": ("Tool1", "left"), "to": ("Tool2", "right")},
            {"from": ("DT", "bottom"), "to": ("Tool2", "bottom")},
        ]
        self._run_engine(items, connections, item_instances)
        self._assert_resource_args(mock_ds.execute.call_args_list, [[[], []]])
        assert mock_ds.filter_id == ""
        expected_fw_resource1 = ProjectItemResource("DS", "database", "label", url)
        expected_filter_stack = (scenario_filter_config("scenario1"),)
        expected_fw_resource1.metadata = {"filter_stack": expected_filter_stack, "filter_id": ""}
        ds_execution_args = [[[expected_fw_resource1], []]]
        self._assert_resource_args(mock_dt.execute.call_args_list, ds_execution_args)
        assert mock_dt.filter_id == "scenario1 - DS"
        filtered_transformed_url = append_filter_config(transformed_url, scenario_filter_config("scenario1"))
        expected_fw_resource2 = ProjectItemResource("DT", "database", "<transformed url>@DT", filtered_transformed_url)
        expected_fw_resource2.metadata = {"filter_stack": expected_filter_stack, "filter_id": "scenario1 - DS"}
        tool1_execution_args = [[[expected_fw_resource2], []]]
        self._assert_resource_args(
            mock_tool1.execute.call_args_list, tool1_execution_args, clear_url_resource_filters=False
        )
        assert mock_tool1.filter_id == "scenario1 - DT"
        expected_fw_resource3 = ProjectItemResource("Tool1", "file", "label_1")
        expected_fw_resource3.metadata = {"filter_stack": expected_filter_stack, "filter_id": "scenario1 - DT"}
        tool2_execution_args = [[[expected_fw_resource2, expected_fw_resource3], []]]
        self._assert_resource_args(
            mock_tool2.execute.call_args_list, tool2_execution_args, clear_url_resource_filters=False
        )
        assert mock_tool2.filter_id == "scenario1 - DT"

    def test_same_filter_stacks_get_merged_for_output_resources(self, tmp_path):
        """Tests this type of workflow:

        DS -> DT -> Tool1 -> Tool2 -> Tool3
               |               |       |
               +---------------+       |
               +-----------------------+
        Tool3 is expected to get DB resources from DT and file resources from Tool2 under a single filter.
        """
        url = "sqlite:///" + str(tmp_path / "db.sqlite")
        with DatabaseMapping(url, create=True) as db_map:
            db_map.add_scenario(name="scenario1")
            db_map.commit_session("Add test data.")
        db_map.close()
        url_fw = _make_url_resource(url)
        mock_ds = self._mock_item("DS", resources_forward=[url_fw], resources_backward=[])
        transformed_url = append_filter_config(url, scenario_filter_config("scenario1"))
        transformed_url = append_filter_config(transformed_url, entity_class_renamer_config(unit="not_node"))
        transformed_url_fw = database_resource("DT", transformed_url, "<transformed url>@DT")
        mock_dt = self._mock_item("DT", resources_forward=[transformed_url_fw])
        file_resource1 = ProjectItemResource("Tool1", "file", "label_1")
        mock_tool1 = self._mock_item("Tool1", resources_forward=[file_resource1])
        file_resource2 = ProjectItemResource("Tool2", "file", "label_2")
        mock_tool2 = self._mock_item("Tool2", resources_forward=[file_resource2])
        mock_tool3 = self._mock_item("Tool3")
        item_instances = {
            "DS": [mock_ds],
            "DT": [mock_dt],
            "Tool1": [mock_tool1],
            "Tool2": [mock_tool2],
            "Tool3": [mock_tool3],
        }
        items = {
            "DS": {"type": "TestItem"},
            "DT": {"type": "TestItem"},
            "Tool1": {"type": "TestItem"},
            "Tool2": {"type": "TestItem"},
            "Tool3": {"type": "TestItem"},
        }
        connections = [
            {
                "from": ("DS", "left"),
                "to": ("DT", "right"),
                "filter_settings": FilterSettings({url_fw.label: {"scenario_filter": {"scenario1": True}}}).to_dict(),
            },
            {
                "from": ("DT", "left"),
                "to": ("Tool1", "right"),
            },
            {"from": ("Tool1", "left"), "to": ("Tool2", "right")},
            {"from": ("DT", "bottom"), "to": ("Tool2", "bottom")},
            {"from": ("Tool2", "left"), "to": ("Tool3", "right")},
            {"from": ("DT", "bottom"), "to": ("Tool3", "bottom")},
        ]
        self._run_engine(items, connections, item_instances)
        self._assert_resource_args(mock_ds.execute.call_args_list, [[[], []]])
        assert mock_ds.filter_id == ""
        expected_fw_resource1 = ProjectItemResource("DS", "database", "label", url)
        expected_filter_stack = (scenario_filter_config("scenario1"),)
        expected_fw_resource1.metadata = {"filter_stack": expected_filter_stack, "filter_id": ""}
        ds_execution_args = [[[expected_fw_resource1], []]]
        self._assert_resource_args(mock_dt.execute.call_args_list, ds_execution_args)
        assert mock_dt.filter_id == "scenario1 - DS"
        filtered_transformed_url = append_filter_config(transformed_url, scenario_filter_config("scenario1"))
        expected_fw_resource2 = ProjectItemResource("DT", "database", "<transformed url>@DT", filtered_transformed_url)
        expected_fw_resource2.metadata = {"filter_stack": expected_filter_stack, "filter_id": "scenario1 - DS"}
        tool1_execution_args = [[[expected_fw_resource2], []]]
        self._assert_resource_args(
            mock_tool1.execute.call_args_list, tool1_execution_args, clear_url_resource_filters=False
        )
        assert mock_tool1.filter_id == "scenario1 - DT"
        expected_fw_resource3 = ProjectItemResource("Tool1", "file", "label_1")
        expected_fw_resource3.metadata = {"filter_stack": expected_filter_stack, "filter_id": "scenario1 - DT"}
        tool2_execution_args = [[[expected_fw_resource2, expected_fw_resource3], []]]
        self._assert_resource_args(
            mock_tool2.execute.call_args_list, tool2_execution_args, clear_url_resource_filters=False
        )
        assert mock_tool2.filter_id == "scenario1 - DT"
        expected_fw_resource4 = ProjectItemResource("Tool2", "file", "label_2")
        expected_fw_resource4.metadata = {"filter_stack": expected_filter_stack, "filter_id": "scenario1 - DT"}
        tool3_execution_args = [[[expected_fw_resource2, expected_fw_resource4], []]]
        self._assert_resource_args(
            mock_tool3.execute.call_args_list, tool3_execution_args, clear_url_resource_filters=False
        )
        assert mock_tool3.filter_id == "scenario1 - DT"

    def test_similar_filter_stacks_get_merged_for_output_resources(self, tmp_path):
        """Tests this type of workflow:

            +-> DT1 -> Tool1 ----+
        DS -+                    |
            +-> DT2 -> Tool2 -> Tool3 -> Tool4
                 |                         |
                 +-------------------------+
        """
        url = "sqlite:///" + str(tmp_path / "db.sqlite")
        with DatabaseMapping(url, create=True) as db_map:
            db_map.add_scenario(name="scenario1")
            db_map.add_scenario(name="scenario2")
            db_map.commit_session("Add test data.")
        db_map.close()
        url_fw = _make_url_resource(url)
        mock_ds = self._mock_item("DS", resources_forward=[url_fw], resources_backward=[])
        transformed_url1 = append_filter_config(url, scenario_filter_config("scenario1"))
        transformed_url1 = append_filter_config(transformed_url1, entity_class_renamer_config(unit="not_node"))
        transformed_url_fw1 = database_resource("DT1", transformed_url1, "<transformed url>@DT1")
        mock_dt1 = self._mock_item("DT1", resources_forward=[transformed_url_fw1])
        transformed_url2 = append_filter_config(url, scenario_filter_config("scenario2"))
        transformed_url2 = append_filter_config(transformed_url2, entity_class_renamer_config(node="not_unit"))
        transformed_url_fw2 = database_resource("DT2", transformed_url2, "<transformed url>@DT2")
        mock_dt2 = self._mock_item("DT2", resources_forward=[transformed_url_fw2])
        file_resource1 = ProjectItemResource("Tool1", "file", "label_1")
        mock_tool1 = self._mock_item("Tool1", resources_forward=[file_resource1])
        file_resource2 = ProjectItemResource("Tool2", "file", "label_2")
        mock_tool2 = self._mock_item("Tool2", resources_forward=[file_resource2])
        file_resource3 = ProjectItemResource("Tool3", "file", "label_3")
        mock_tool3 = self._mock_item("Tool3", resources_forward=[file_resource3])
        mock_tool4 = self._mock_item("Tool4")
        item_instances = {
            "DS": [mock_ds],
            "DT1": [mock_dt1],
            "DT2": [mock_dt2],
            "Tool1": [mock_tool1],
            "Tool2": [mock_tool2],
            "Tool3": [mock_tool3],
            "Tool4": [mock_tool4],
        }
        items = {
            "DS": {"type": "TestItem"},
            "DT1": {"type": "TestItem"},
            "DT2": {"type": "TestItem"},
            "Tool1": {"type": "TestItem"},
            "Tool2": {"type": "TestItem"},
            "Tool3": {"type": "TestItem"},
            "Tool4": {"type": "TestItem"},
        }
        connections = [
            {
                "from": ("DS", "right"),
                "to": ("DT1", "left"),
                "filter_settings": FilterSettings(
                    {url_fw.label: {SCENARIO_FILTER_TYPE: {"scenario1": True, "scenario2": False}}}
                ).to_dict(),
            },
            {
                "from": ("DT1", "right"),
                "to": ("Tool1", "left"),
            },
            {"from": ("Tool1", "left"), "to": ("Tool3", "top")},
            {
                "from": ("DS", "right"),
                "to": ("DT2", "left"),
                "filter_settings": FilterSettings(
                    {url_fw.label: {SCENARIO_FILTER_TYPE: {"scenario1": False, "scenario2": True}}}
                ).to_dict(),
            },
            {"from": ("DT2", "right"), "to": ("Tool2", "left")},
            {"from": ("Tool2", "right"), "to": ("Tool3", "left")},
            {"from": ("Tool3", "right"), "to": ("Tool4", "left")},
            {"from": ("DT2", "bottom"), "to": ("Tool4", "bottom")},
        ]
        self._run_engine(items, connections, item_instances)
        assert all(instance_list == [] for instance_list in item_instances.values())
        self._assert_resource_args(mock_ds.execute.call_args_list, [[[], []]])
        assert mock_ds.filter_id == ""
        expected_fw_resource1 = ProjectItemResource("DS", "database", "label", url)
        expected_filter_stack1 = (scenario_filter_config("scenario1"),)
        expected_fw_resource1.metadata = {"filter_stack": expected_filter_stack1, "filter_id": ""}
        ds_execution_args1 = [[[expected_fw_resource1], []]]
        self._assert_resource_args(mock_dt1.execute.call_args_list, ds_execution_args1)
        assert mock_dt1.filter_id == "scenario1 - DS"
        filtered_transformed_url1 = append_filter_config(transformed_url1, scenario_filter_config("scenario1"))
        expected_fw_resource2 = ProjectItemResource(
            "DT1", "database", "<transformed url>@DT1", filtered_transformed_url1
        )
        expected_fw_resource2.metadata = {"filter_stack": expected_filter_stack1, "filter_id": "scenario1 - DS"}
        tool1_execution_args = [[[expected_fw_resource2], []]]
        self._assert_resource_args(
            mock_tool1.execute.call_args_list, tool1_execution_args, clear_url_resource_filters=False
        )
        assert mock_tool1.filter_id == "scenario1 - DT1"
        expected_fw_resource3 = ProjectItemResource("DS", "database", "label", url)
        expected_filter_stack2 = (scenario_filter_config("scenario2"),)
        expected_fw_resource3.metadata = {"filter_stack": expected_filter_stack2, "filter_id": ""}
        ds_execution_args2 = [[[expected_fw_resource3], []]]
        self._assert_resource_args(mock_dt2.execute.call_args_list, ds_execution_args2)
        assert mock_dt2.filter_id == "scenario2 - DS"
        filtered_transformed_url2 = append_filter_config(transformed_url2, scenario_filter_config("scenario2"))
        expected_fw_resource4 = ProjectItemResource(
            "DT2", "database", "<transformed url>@DT2", filtered_transformed_url2
        )
        expected_fw_resource4.metadata = {"filter_stack": expected_filter_stack2, "filter_id": "scenario2 - DS"}
        tool2_execution_args = [[[expected_fw_resource4], []]]
        self._assert_resource_args(
            mock_tool2.execute.call_args_list, tool2_execution_args, clear_url_resource_filters=False
        )
        assert mock_tool2.filter_id == "scenario2 - DT2"
        expected_fw_resource5 = ProjectItemResource("Tool1", "file", "label_1")
        expected_fw_resource5.metadata = {"filter_stack": expected_filter_stack1, "filter_id": "scenario1 - DT1"}
        expected_fw_resource6 = ProjectItemResource("Tool2", "file", "label_2")
        expected_fw_resource6.metadata = {"filter_stack": expected_filter_stack2, "filter_id": "scenario2 - DT2"}
        tool3_execution_args = [[[expected_fw_resource5, expected_fw_resource6], []]]
        self._assert_resource_args(
            mock_tool3.execute.call_args_list, tool3_execution_args, clear_url_resource_filters=False
        )
        assert mock_tool3.filter_id == "scenario1 - DT1 & scenario2 - DT2"
        expected_fw_resource7 = ProjectItemResource("Tool3", "file", "label_3")
        expected_fw_resource7.metadata = {
            "filter_stack": expected_filter_stack1 + expected_filter_stack2,
            "filter_id": "scenario1 - DT1 & scenario2 - DT2",
        }

        tool4_execution_args = [[[expected_fw_resource7, expected_fw_resource4], []]]
        self._assert_resource_args(
            mock_tool4.execute.call_args_list, tool4_execution_args, clear_url_resource_filters=False
        )
        assert mock_tool4.filter_id == "scenario1 - DT1 & scenario2 - DT2 & scenario2 - DT2"

    def test_self_jump_succeeds(self):
        mock_item = self._mock_item("item", resources_forward=[], resources_backward=[])
        items = {"item": {"type": "TestItem"}}
        connections = []
        jumps = [Jump("item", "bottom", "item", "top", self._LOOP_TWICE).to_dict()]
        execution_permits = {"item": True}

        engine = SpineEngine(
            items=items,
            connections=connections,
            jumps=jumps,
            execution_permits=execution_permits,
            items_module_name="items_module",
        )
        engine.make_item = lambda name, direction: mock_item
        engine.run()
        lock_1 = mock_item.execute.call_args_list[0].args[-1]
        lock_2 = mock_item.execute.call_args_list[1].args[-1]
        assert mock_item.execute.call_args_list == [call([], [], lock_1), call([], [], lock_2)]
        assert engine.state() == SpineEngineState.COMPLETED

    def test_jump_resources_get_passed_correctly(self, tmp_path):
        url_fw_a = "sqlite:///" + str(tmp_path / "fw_a")
        url_bw_a = "sqlite:///" + str(tmp_path / "bw_a")
        url_fw_b = "sqlite:///" + str(tmp_path / "fw_b")
        url_bw_b = "sqlite:///" + str(tmp_path / "bw_b")
        url_fw_c = "sqlite:///" + str(tmp_path / "fw_c")
        url_bw_c = "sqlite:///" + str(tmp_path / "bw_c")
        url_fw_d = "sqlite:///" + str(tmp_path / "fw_d")
        url_bw_d = "sqlite:///" + str(tmp_path / "bw_d")
        resource_fw_a = _make_url_resource(url_fw_a)
        resource_bw_a = _make_url_resource(url_bw_a)
        item_a = self._mock_item("a", resources_forward=[resource_fw_a], resources_backward=[resource_bw_a])
        resource_fw_b = _make_url_resource(url_fw_b)
        resource_bw_b = _make_url_resource(url_bw_b)
        item_b = self._mock_item("b", resources_forward=[resource_fw_b], resources_backward=[resource_bw_b])
        resource_fw_c = _make_url_resource(url_fw_c)
        resource_bw_c = _make_url_resource(url_bw_c)
        item_c = self._mock_item("c", resources_forward=[resource_fw_c], resources_backward=[resource_bw_c])
        resource_fw_d = _make_url_resource(url_fw_d)
        resource_bw_d = _make_url_resource(url_bw_d)
        item_d = self._mock_item("d", resources_forward=[resource_fw_d], resources_backward=[resource_bw_d])
        item_instances = {"a": [item_a], "b": [item_b, item_b], "c": [item_c, item_c], "d": [item_d]}
        items = {
            "a": {"type": "TestItem"},
            "b": {"type": "TestItem"},
            "c": {"type": "TestItem"},
            "d": {"type": "TestItem"},
        }
        connections = [
            c.to_dict()
            for c in (
                Connection("a", "right", "b", "left"),
                Connection("b", "bottom", "c", "top"),
                Connection("c", "left", "d", "right"),
            )
        ]
        jumps = [Jump("c", "right", "b", "right", self._LOOP_TWICE).to_dict()]
        self._run_engine(items, connections, item_instances, jumps=jumps)
        self._assert_resource_args(
            item_a.execute.call_args_list, [[[], [self._default_backward_url_resource(url_bw_b, "a", "b")]]]
        )
        self._assert_resource_args(
            item_b.execute.call_args_list,
            2
            * [
                [
                    [self._default_forward_url_resource(url_fw_a, "a")],
                    [self._default_backward_url_resource(url_bw_c, "b", "c")],
                ]
            ],
        )
        self._assert_resource_args(
            item_c.execute.call_args_list,
            2
            * [
                [
                    [self._default_forward_url_resource(url_fw_b, "b")],
                    [self._default_backward_url_resource(url_bw_d, "c", "d")],
                ]
            ],
        )
        self._assert_resource_args(
            item_d.execute.call_args_list, [[[self._default_forward_url_resource(url_fw_c, "c")], []]]
        )

    def test_nested_jump_with_inner_self_jump(self, tmp_path):
        url_fw_a = "sqlite:///" + str(tmp_path / "fw_a")
        url_bw_a = "sqlite:///" + str(tmp_path / "bw_a")
        url_fw_b = "sqlite:///" + str(tmp_path / "fw_b")
        url_bw_b = "sqlite:///" + str(tmp_path / "bw_b")
        url_fw_c = "sqlite:///" + str(tmp_path / "fw_c")
        url_bw_c = "sqlite:///" + str(tmp_path / "bw_c")
        resource_fw_a = _make_url_resource(url_fw_a)
        resource_bw_a = _make_url_resource(url_bw_a)
        item_a = self._mock_item("a", resources_forward=[resource_fw_a], resources_backward=[resource_bw_a])
        resource_fw_b = _make_url_resource(url_fw_b)
        resource_bw_b = _make_url_resource(url_bw_b)
        item_b = self._mock_item("b", resources_forward=[resource_fw_b], resources_backward=[resource_bw_b])
        resource_fw_c = _make_url_resource(url_fw_c)
        resource_bw_c = _make_url_resource(url_bw_c)
        item_c = self._mock_item("c", resources_forward=[resource_fw_c], resources_backward=[resource_bw_c])
        item_instances = {"a": 2 * [item_a], "b": 4 * [item_b], "c": 2 * [item_c]}
        items = {"a": {"type": "TestItem"}, "b": {"type": "TestItem"}, "c": {"type": "TestItem"}}
        connections = [
            c.to_dict() for c in (Connection("a", "right", "b", "left"), Connection("b", "bottom", "c", "top"))
        ]
        jumps = [
            Jump("c", "right", "a", "right", self._LOOP_TWICE).to_dict(),
            Jump("b", "top", "b", "top", self._LOOP_TWICE).to_dict(),
        ]
        self._run_engine(items, connections, item_instances, jumps=jumps)
        expected = 2 * [[[], [self._default_backward_url_resource(url_bw_b, "a", "b")]]]
        self._assert_resource_args(item_a.execute.call_args_list, expected)
        expected = 4 * [
            [
                [self._default_forward_url_resource(url_fw_a, "a")],
                [self._default_backward_url_resource(url_bw_c, "b", "c")],
            ]
        ]
        self._assert_resource_args(item_b.execute.call_args_list, expected)
        expected = 2 * [[[self._default_forward_url_resource(url_fw_b, "b")], []]]
        self._assert_resource_args(item_c.execute.call_args_list, expected)

    def test_stopping_execution_in_the_middle_of_a_loop_does_not_leave_multithread_executor_running(self):
        item_a = self._mock_item("a")
        item_b = self._mock_item("b")
        item_instances = {"a": [item_a, item_a, item_a, item_a], "b": [item_b]}
        items = {
            "a": {"type": "TestItem"},
            "b": {"type": "TestItem"},
        }
        connections = [c.to_dict() for c in (Connection("a", "right", "b", "left"),)]
        jumps = [Jump("a", "right", "a", "right", self._LOOP_FOREVER).to_dict()]
        engine = self._create_engine(items, connections, item_instances, jumps=jumps)

        def execute_item_a(loop_counter, *args, **kwargs):
            if loop_counter[0] == 2:
                engine.stop()
                return ItemExecutionFinishState.STOPPED
            loop_counter[0] += 1
            return ItemExecutionFinishState.SUCCESS

        loop_counter = [0]
        item_a.execute.side_effect = partial(execute_item_a, loop_counter)
        engine.run()
        assert engine.state() == SpineEngineState.USER_STOPPED
        assert item_a.execute.call_count == 3
        item_b.execute.assert_not_called()

    def test_executing_loop_source_item_only_does_not_execute_the_loop(self):
        item_a = self._mock_item("a")
        item_b = self._mock_item("b")
        item_instances = {"a": [item_a, item_a, item_a, item_a], "b": [item_b, item_b, item_b, item_b]}
        items = {
            "a": {"type": "TestItem"},
            "b": {"type": "TestItem"},
        }
        connections = [c.to_dict() for c in (Connection("a", "right", "b", "left"),)]
        jumps = [Jump("b", "right", "a", "right", self._LOOP_FOREVER).to_dict()]
        self._run_engine(items, connections, item_instances, execution_permits={"a": False, "b": True}, jumps=jumps)
        assert item_a.execute.call_count == 0
        assert item_b.execute.call_count == 1

    @staticmethod
    def _assert_resource_args(arg_packs, expected_packs, clear_url_resource_filters=True):
        assert len(arg_packs) == len(expected_packs)
        for pack, expected_pack in zip(arg_packs, expected_packs):
            assert len(pack) == len(expected_pack)
            for args, expected in zip(pack[0], expected_pack):
                assert len(args) == len(expected)
                for resource, expected_resource in zip(args, expected):
                    assert resource.provider_name == expected_resource.provider_name
                    assert resource.label == expected_resource.label
                    url = clear_filter_configs(resource.url) if clear_url_resource_filters else resource.url
                    assert url == expected_resource.url
                    for key, value in expected_resource.metadata.items():
                        assert resource.metadata[key] == value


class TestValidateSingleJump(unittest.TestCase):
    def test_bug(self):
        edges = {"a": ["b"], "b": ["c"], "c": "d"}
        dag = make_dag(edges)
        jumps = [Jump("b", "top", "a", "top"), Jump("d", "top", "c", "top")]
        jump_to_check = jumps[0]
        try:
            validate_single_jump(jump_to_check, jumps, dag)
        except EngineInitFailed:
            self.fail("validate_single_jump shouldn't have raised")
