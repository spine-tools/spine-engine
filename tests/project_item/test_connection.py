######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""
Uni tests for the ``connection`` module.

:authors: A. Soininen (VTT)
:date:    18.2.2021
"""
import os.path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock
from spinedb_api import DatabaseMapping, import_scenarios, import_tools, import_alternatives, import_object_classes
from spine_engine.project_item.connection import Connection, FilterSettings, Jump
from spine_engine.project_item.project_item_resource import database_resource
from spine_engine.spine_engine import SpineEngine
from spinedb_api.filters.scenario_filter import SCENARIO_FILTER_TYPE
from spinedb_api.filters.tool_filter import TOOL_FILTER_TYPE


class TestConnection(unittest.TestCase):
    def test_serialization_without_filters(self):
        connection = Connection("source", "bottom", "destination", "top", {"option": 23})
        connection_dict = connection.to_dict()
        restored = Connection.from_dict(connection_dict)
        self.assertEqual(restored.source, "source")
        self.assertEqual(restored.source_position, "bottom")
        self.assertEqual(restored.destination, "destination")
        self.assertEqual(restored.destination_position, "top")
        self.assertEqual(restored.options, {"option": 23})

    def test_ready_to_execute_returns_false_when_required_filters_do_not_exist(self):
        options = {"require_" + SCENARIO_FILTER_TYPE: True}
        connection = Connection("source", "bottom", "destination", "top", options)
        self.assertFalse(connection.ready_to_execute())

    def test_ready_to_execute_returns_false_when_required_filters_are_offline(self):
        options = {"require_" + SCENARIO_FILTER_TYPE: True}
        filter_settings = FilterSettings({"database@Data Store": {SCENARIO_FILTER_TYPE: {"scenario_1": False}}})
        connection = Connection("source", "bottom", "destination", "top", options, filter_settings)
        self.assertFalse(connection.ready_to_execute())

    def test_ready_to_execute_returns_true_when_required_filters_are_online(self):
        options = {"require_" + SCENARIO_FILTER_TYPE: True}
        filter_settings = FilterSettings({"database@Data Store": {SCENARIO_FILTER_TYPE: {"scenario_1": True}}})
        connection = Connection("source", "bottom", "destination", "top", options, filter_settings)
        self.assertTrue(connection.ready_to_execute())

    def test_require_filter_online(self):
        options = {"require_" + SCENARIO_FILTER_TYPE: True}
        filter_settings = FilterSettings({"database@Data Store": {SCENARIO_FILTER_TYPE: {"scenario_1": True}}})
        connection = Connection("source", "bottom", "destination", "top", options, filter_settings)
        self.assertTrue(connection.require_filter_online(SCENARIO_FILTER_TYPE))
        self.assertFalse(connection.require_filter_online(TOOL_FILTER_TYPE))

    def test_require_filter_online_default_value(self):
        filter_settings = FilterSettings({"database@Data Store": {SCENARIO_FILTER_TYPE: {"scenario_1": True}}})
        connection = Connection("source", "bottom", "destination", "top", {}, filter_settings)
        self.assertIsNotNone(connection.require_filter_online(SCENARIO_FILTER_TYPE))
        self.assertIsNotNone(connection.require_filter_online(TOOL_FILTER_TYPE))
        self.assertFalse(connection.require_filter_online(SCENARIO_FILTER_TYPE))
        self.assertFalse(connection.require_filter_online(TOOL_FILTER_TYPE))

    def test_notification_when_filter_validation_fails(self):
        options = {"require_" + SCENARIO_FILTER_TYPE: True, "require_" + TOOL_FILTER_TYPE: True}
        filter_settings = FilterSettings()
        connection = Connection("source", "bottom", "destination", "top", options, filter_settings)
        self.assertEqual(
            connection.notifications(),
            ["At least one scenario filter must be active.", "At least one tool filter must be active."],
        )

    def test_nothing_to_notify(self):
        filter_settings = FilterSettings(
            {"database@Data Store": {SCENARIO_FILTER_TYPE: {"scenario_1": True}, TOOL_FILTER_TYPE: {"tool_1": True}}}
        )
        options = {"require_" + SCENARIO_FILTER_TYPE: True, "require_" + TOOL_FILTER_TYPE: True}
        connection = Connection("source", "bottom", "destination", "top", options, filter_settings)
        self.assertEqual(connection.notifications(), [])


class TestConnectionWithDatabase(unittest.TestCase):
    def setUp(self):
        self._temp_dir = TemporaryDirectory()
        self._url = "sqlite:///" + os.path.join(self._temp_dir.name, "db.sqlite")
        self._db_map = DatabaseMapping(self._url, create=True)

    def tearDown(self):
        self._db_map.connection.close()
        self._temp_dir.cleanup()

    def test_serialization_with_filters(self):
        import_scenarios(self._db_map, ("my_scenario",))
        self._db_map.commit_session("Add test data.")
        filter_settings = FilterSettings(
            {"my_database": {"scenario_filter": {"my_scenario": False}}}, auto_online=False
        )
        connection = Connection("source", "bottom", "destination", "top", filter_settings=filter_settings)
        connection.receive_resources_from_source([database_resource("unit_test", self._url, "my_database")])
        connection_dict = connection.to_dict()
        restored = Connection.from_dict(connection_dict)
        self.assertEqual(restored.source, "source")
        self.assertEqual(restored.source_position, "bottom")
        self.assertEqual(restored.destination, "destination")
        self.assertEqual(restored.destination_position, "top")
        self.assertEqual(restored.options, {})
        self.assertEqual(restored._filter_settings, filter_settings)

    def test_enabled_scenarios_with_auto_enable_on(self):
        import_scenarios(self._db_map, ("scenario_1", "scenario_2"))
        self._db_map.commit_session("Add test data.")
        filter_settings = FilterSettings({"my_database": {"scenario_filter": {"scenario_1": False}}})
        connection = Connection("source", "bottom", "destination", "top", filter_settings=filter_settings)
        resources = [database_resource("unit_test", self._url, "my_database", filterable=True)]
        connection.receive_resources_from_source(resources)
        self.assertEqual(connection.enabled_filters("my_database"), {"scenario_filter": ["scenario_2"]})

    def test_enabled_scenarios_with_auto_enable_off(self):
        import_scenarios(self._db_map, ("scenario_1", "scenario_2"))
        self._db_map.commit_session("Add test data.")
        filter_settings = FilterSettings({"my_database": {"scenario_filter": {"scenario_1": True}}}, auto_online=False)
        connection = Connection("source", "bottom", "destination", "top", filter_settings=filter_settings)
        resources = [database_resource("unit_test", self._url, "my_database", filterable=True)]
        connection.receive_resources_from_source(resources)
        self.assertEqual(connection.enabled_filters("my_database"), {"scenario_filter": ["scenario_1"]})

    def test_enabled_tools_with_auto_enable_on(self):
        import_tools(self._db_map, ("tool_1", "tool_2"))
        self._db_map.commit_session("Add test data.")
        filter_settings = FilterSettings({"my_database": {"tool_filter": {"tool_1": False}}})
        connection = Connection("source", "bottom", "destination", "top", filter_settings=filter_settings)
        resources = [database_resource("unit_test", self._url, "my_database", filterable=True)]
        connection.receive_resources_from_source(resources)
        self.assertEqual(connection.enabled_filters("my_database"), {"tool_filter": ["tool_2"]})

    def test_enabled_tools_with_auto_enable_off(self):
        import_tools(self._db_map, ("tool_1", "tool_2"))
        self._db_map.commit_session("Add test data.")
        filter_settings = FilterSettings({"my_database": {"tool_filter": {"tool_1": True}}}, auto_online=False)
        connection = Connection("source", "bottom", "destination", "top", filter_settings=filter_settings)
        resources = [database_resource("unit_test", self._url, "my_database", filterable=True)]
        connection.receive_resources_from_source(resources)
        self.assertEqual(connection.enabled_filters("my_database"), {"tool_filter": ["tool_1"]})

    def test_purge_data_before_writing(self):
        import_alternatives(self._db_map, ("my_alternative",))
        import_object_classes(self._db_map, ("my_object_class",))
        self._db_map.commit_session("Add test data.")
        self._db_map.connection.close()
        connection = Connection(
            "source",
            "bottom",
            "destination",
            "top",
            options={"purge_before_writing": True, "purge_settings": {"object_class": True}},
        )
        resources = [database_resource("unit_test", self._url, "my_database")]
        connection.clean_up_backward_resources(resources)
        database_map = DatabaseMapping(self._url)
        object_class_list = database_map.query(database_map.object_class_sq).all()
        self.assertEqual(len(object_class_list), 0)
        alternative_list = database_map.query(database_map.alternative_sq).all()
        self.assertEqual(len(alternative_list), 2)
        self.assertEqual(alternative_list[0].name, "Base")
        self.assertEqual(alternative_list[1].name, "my_alternative")
        database_map.connection.close()


class TestJump(unittest.TestCase):
    def test_default_condition_prevents_jump(self):
        jump = Jump("source", "bottom", "destination", "top")
        self.assertFalse(jump.is_condition_true(1))

    def test_empty_condition_prevents_jump(self):
        jump = Jump("source", "bottom", "destination", "top", {"type": "python-script", "script": ""})
        self.assertFalse(jump.is_condition_true(1))

    def test_counter_passed_to_condition(self):
        condition = {
            "type": "python-script",
            "script": "\n".join(("import sys", "counter = int(sys.argv[1])", "exit(0 if counter == 23 else 1)")),
        }
        jump = Jump("source", "bottom", "destination", "top", condition)
        jump.make_logger(Mock())
        self.assertTrue(jump.is_condition_true(23))

    @unittest.skip("Doesn't work in github actions: ModuleNotFoundError: No module named 'spine_items'")
    def test_tool_spec_condition(self):
        condition = {"type": "tool-specification", "specification": "loop_twice"}
        jump = Jump("source", "bottom", "destination", "top", condition)
        jump.make_logger(Mock())
        with TemporaryDirectory() as temp_dir:
            main_program_file_path = "script"
            temp_file_path = os.path.join(temp_dir, main_program_file_path)
            with open(temp_file_path, "w+") as program_file:
                program_file.write('if ARGS[1] == "23" exit(0) else exit(1) end')
            engine = SpineEngine(
                settings={"appSettings/useJuliaKernel": "1"},
                project_dir=temp_dir,
                specifications={
                    "Tool": [
                        {
                            "name": "loop_twice",
                            "tooltype": "julia",
                            "includes_main_path": temp_dir,
                            "includes": [main_program_file_path],
                        }
                    ]
                },
            )
            jump.prepare_condition(engine)
            self.assertTrue(jump.is_condition_true(23))

    def test_dictionary(self):
        jump = Jump("source", "bottom", "destination", "top", {"type": "python-script", "script": "exit(23)"})
        jump_dict = jump.to_dict()
        new_jump = Jump.from_dict(jump_dict)
        self.assertEqual(new_jump.source, jump.source)
        self.assertEqual(new_jump.destination, jump.destination)
        self.assertEqual(new_jump.source_position, jump.source_position)
        self.assertEqual(new_jump.destination_position, jump.destination_position)
        self.assertEqual(new_jump.condition, jump.condition)


class TestFilterSettings(unittest.TestCase):
    def test_has_filters_returns_false_when_no_filters_exist(self):
        settings = FilterSettings()
        self.assertFalse(settings.has_filters())

    def test_has_filters_returns_true_when_filters_exist(self):
        settings = FilterSettings({"database@Data Store": {TOOL_FILTER_TYPE: {"tool_1": True}}})
        self.assertTrue(settings.has_filters())

    def test_has_filters_online_returns_false_when_no_filters_exist(self):
        settings = FilterSettings()
        self.assertFalse(settings.has_filter_online(SCENARIO_FILTER_TYPE))

    def test_has_filters_online_returns_false_when_filters_are_offline(self):
        settings = FilterSettings(
            {"database@Data Store": {SCENARIO_FILTER_TYPE: {"scenario_1": True}, TOOL_FILTER_TYPE: {"tool_1": False}}}
        )
        self.assertFalse(settings.has_filter_online(TOOL_FILTER_TYPE))

    def test_has_filters_online_returns_true_when_filters_are_online(self):
        settings = FilterSettings(
            {"database@Data Store": {SCENARIO_FILTER_TYPE: {"scenario_1": False}, TOOL_FILTER_TYPE: {"tool_1": True}}}
        )
        self.assertTrue(settings.has_filter_online(TOOL_FILTER_TYPE))

    def test_has_filter_online_works_when_there_are_no_known_filters(self):
        settings = FilterSettings()
        self.assertFalse(settings.has_filter_online(SCENARIO_FILTER_TYPE))
        self.assertFalse(settings.has_filter_online(TOOL_FILTER_TYPE))


if __name__ == "__main__":
    unittest.main()
