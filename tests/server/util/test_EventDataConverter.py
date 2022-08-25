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
Unit tests for EventDataConverter class.
:author: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   27.8.2021
"""

import unittest
import json
import ast
from spine_engine.spine_engine import ItemExecutionFinishState
from spine_engine.server.util.event_data_converter import EventDataConverter


class TestEventDataConverter(unittest.TestCase):

    def make_event_data(self):
        test_events = [('exec_started', {'item_name': 'helloworld', 'direction': 'BACKWARD'}),
                       ('exec_started', {'item_name': 'Data Connection 1', 'direction': 'BACKWARD'}),
                       ('exec_finished', {'item_name': 'helloworld', 'direction': 'BACKWARD', 'state': 'RUNNING', 'item_state': ItemExecutionFinishState.SUCCESS}),
                       ('exec_finished', {'item_name': 'Data Connection 1', 'direction': 'BACKWARD', 'state': 'RUNNING', 'item_state': ItemExecutionFinishState.SUCCESS}),
                       ('exec_started', {'item_name': 'Data Connection 1', 'direction': 'FORWARD'}),
                       ('event_msg', {'item_name': 'Data Connection 1', 'filter_id': '', 'msg_type': 'msg_success', 'msg_text': 'Executing Data Connection Data Connection 1 finished'}),
                       ('exec_finished', {'item_name': 'Data Connection 1', 'direction': 'FORWARD', 'state': 'RUNNING', 'item_state': ItemExecutionFinishState.SUCCESS}),
                       ('flash', {'item_name': 'from Data Connection 1 to helloworld'}),
                       ('exec_started', {'item_name': 'helloworld', 'direction': 'FORWARD'}),
                       ('event_msg', {'item_name': 'helloworld', 'filter_id': '', 'msg_type': 'msg', 'msg_text': "*** Executing Tool specification <b>helloworld2</b> in <a style='color:#99CCFF;' title='C:\\data\\GIT\\SPINEENGINE\\spine_engine\\server\\received_projects\\helloworld__35bc62cea0324e8788144ce81342f4f1'href='file:///C:\\data\\GIT\\SPINEENGINE\\spine_engine\\server\\received_projects\\helloworld__35bc62cea0324e8788144ce81342f4f1'>source directory</a> ***"}),
                       ('persistent_execution_msg', {'item_name': 'helloworld', 'filter_id': '', 'type': 'persistent_started', 'key': ('C:\\Python38\\python.exe', 'helloworld'), 'language': 'python'}),
                       ('persistent_execution_msg', {'item_name': 'helloworld', 'filter_id': '', 'type': 'stdin', 'data': '# Running python helloworld.py'}),
                       ('persistent_execution_msg', {'item_name': 'helloworld', 'filter_id': '', 'type': 'stdout', 'data': 'helloo'}),
                       ('event_msg', {'item_name': 'helloworld', 'filter_id': '', 'msg_type': 'msg', 'msg_text': "*** Archiving output files to <a style='color:#BB99FF;' title='C:\\data\\GIT\\SPINEENGINE\\spine_engine\\server\\received_projects\\helloworld__35bc62cea0324e8788144ce81342f4f1\\.spinetoolbox\\items\\helloworld\\output\\2022-08-19T13.03.13' href='file:///C:\\data\\GIT\\SPINEENGINE\\spine_engine\\server\\received_projects\\helloworld__35bc62cea0324e8788144ce81342f4f1\\.spinetoolbox\\items\\helloworld\\output\\2022-08-19T13.03.13'>results directory</a> ***"}),
                       ('exec_finished', {'item_name': 'helloworld', 'direction': 'FORWARD', 'state': 'RUNNING', 'item_state': ItemExecutionFinishState.SUCCESS}),
                       ('dag_exec_finished', 'COMPLETED')]
        return test_events

    def test_convert(self):
        event_data = self.make_event_data()
        json_str = EventDataConverter.convert(event_data)
        converted_events = json.loads(json_str)
        self.assertEqual(16, len(converted_events["items"]))

    def test_two_converts(self):
        event_data = self.make_event_data()
        json_str = EventDataConverter.convert(event_data)
        event_data2 = EventDataConverter.deconvert(json.loads(json_str), True)
        json_str2 = EventDataConverter.convert(event_data2)
        self.assertEqual(json_str, json_str2)

    def test_convert_deconvert(self):
        """Converts server side event list ready for transmission,
        then deconverts the event list like we do in Spine Toolbox (client) side.
        Compares what was sent from the server to what will be received at client."""
        event_data = self.make_event_data()
        json_str = EventDataConverter.convert(event_data)
        converted_events = json.loads(json_str)
        self.assertEqual(16, len(converted_events["items"]))
        deconverted_events = EventDataConverter.deconvert(converted_events, True)
        output_list = list()
        # NOTE: This is from spine_toolbox.spine_engine_manager.RemoteSpineEngineManager
        for event in deconverted_events:
            try:
                # Handle execution state transformation, see returned data from SpineEngine._process_event()
                dict_str = self._add_quotes_to_state_str(event[1])
                data_dict = ast.literal_eval(dict_str)  # ast.literal_eval fails if input isn't a Python datatype
                if "item_state" in data_dict.keys():
                    data_dict["item_state"] = self.transform_execution_state(data_dict["item_state"])
                output_list.append((event[0], data_dict))
            except ValueError:
                # ast.literal_eval throws this if trying to turn a regular string like "COMPLETED" or "FAILED" to
                # a dict. See returned data from SpineEngine._process_event()
                output_list.append((event[0], event[1]))
        self.assertEqual(event_data, output_list)

    # NOTE: This is from spine_toolbox.spine_engine_manager.RemoteSpineEngineManager
    @staticmethod
    def transform_execution_state(state):
        """Transforms state string into an ItemExecutionFinishState enum.

        Args:
            state (str): State as string

        Returns:
            ItemExecutionFinishState: Enum if given str is valid, None otherwise.
        """
        states = dict()
        states["<ItemExecutionFinishState.SUCCESS: 1>"] = ItemExecutionFinishState.SUCCESS
        states["<ItemExecutionFinishState.FAILURE: 1>"] = ItemExecutionFinishState.FAILURE
        states["<ItemExecutionFinishState.SKIPPED: 1>"] = ItemExecutionFinishState.SKIPPED
        states["<ItemExecutionFinishState.EXCLUDED: 1>"] = ItemExecutionFinishState.EXCLUDED
        states["<ItemExecutionFinishState.STOPPED: 1>"] = ItemExecutionFinishState.STOPPED
        states["<ItemExecutionFinishState.NEVER_FINISHED: 1>"] = ItemExecutionFinishState.NEVER_FINISHED
        return states.get(state, None)

    # NOTE: This is from spine_toolbox.spine_engine_manager.RemoteSpineEngineManager
    @staticmethod
    def _add_quotes_to_state_str(s):
        """Makes string ready for ast.literal_eval() by adding quotes around item_state value.
        The point of this method is to add quotes (') around item_state value. E.g.
        'item_state': <ItemExecutionFinishState.SUCCESS: 1> becomes
        'item_state': '<ItemExecutionFinishState.SUCCESS: 1>' because ast.literal_eval
        fails with a SyntaxError if the string contains invalid Python data types.
        Make sure that quotes (') are not added to other < > enclosed substrings, such as
        <b> or </b>. If there's no match for what we are looking for, this method should
         return the original string."""
        state = s.partition("'item_state': <")[2].partition(">")[0]
        new_s = s.replace("<" + state + ">", "\'<" + state + ">\'")
        return new_s


if __name__ == '__main__':
    unittest.main()
