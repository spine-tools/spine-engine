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

"""
Unit tests for EventDataConverter class.
"""

import unittest
from copy import deepcopy
from spine_engine.spine_engine import ItemExecutionFinishState
from spine_engine.server.util.event_data_converter import EventDataConverter


class TestEventDataConverter(unittest.TestCase):
    def make_event_data(self):
        test_events = [
            ('exec_started', {'item_name': 'helloworld', 'direction': 'BACKWARD'}),
            ('exec_started', {'item_name': 'Data Connection 1', 'direction': 'BACKWARD'}),
            (
                'exec_finished',
                {
                    'item_name': 'helloworld',
                    'direction': 'BACKWARD',
                    'state': 'RUNNING',
                    'item_state': ItemExecutionFinishState.SUCCESS,
                },
            ),
            (
                'exec_finished',
                {
                    'item_name': 'Data Connection 1',
                    'direction': 'BACKWARD',
                    'state': 'RUNNING',
                    'item_state': ItemExecutionFinishState.SUCCESS,
                },
            ),
            ('exec_started', {'item_name': 'Data Connection 1', 'direction': 'FORWARD'}),
            (
                'event_msg',
                {
                    'item_name': 'Data Connection 1',
                    'filter_id': '',
                    'msg_type': 'msg_success',
                    'msg_text': 'Executing Data Connection Data Connection 1 finished',
                },
            ),
            (
                'exec_finished',
                {
                    'item_name': 'Data Connection 1',
                    'direction': 'FORWARD',
                    'state': 'RUNNING',
                    'item_state': ItemExecutionFinishState.SUCCESS,
                },
            ),
            ('flash', {'item_name': 'from Data Connection 1 to helloworld'}),
            ('exec_started', {'item_name': 'helloworld', 'direction': 'FORWARD'}),
            (
                'event_msg',
                {
                    'item_name': 'helloworld',
                    'filter_id': '',
                    'msg_type': 'msg',
                    'msg_text': "*** Executing Tool specification <b>helloworld2</b> in <a style='color:#99CCFF;' title='C:\\data\\GIT\\SPINEENGINE\\spine_engine\\server\\received_projects\\helloworld__35bc62cea0324e8788144ce81342f4f1'href='file:///C:\\data\\GIT\\SPINEENGINE\\spine_engine\\server\\received_projects\\helloworld__35bc62cea0324e8788144ce81342f4f1'>source directory</a> ***",
                },
            ),
            (
                'persistent_execution_msg',
                {
                    'item_name': 'helloworld',
                    'filter_id': '',
                    'type': 'persistent_started',
                    'key': '6ceeb59271114fc2a0f787266f72dedc',
                    'language': 'python',
                },
            ),
            (
                'persistent_execution_msg',
                {'item_name': 'helloworld', 'filter_id': '', 'type': 'stdin', 'data': '# Running python helloworld.py'},
            ),
            (
                'persistent_execution_msg',
                {'item_name': 'helloworld', 'filter_id': '', 'type': 'stdout', 'data': 'helloo'},
            ),
            (
                'event_msg',
                {
                    'item_name': 'helloworld',
                    'filter_id': '',
                    'msg_type': 'msg',
                    'msg_text': "*** Archiving output files to <a style='color:#BB99FF;' title='C:\\data\\GIT\\SPINEENGINE\\spine_engine\\server\\received_projects\\helloworld__35bc62cea0324e8788144ce81342f4f1\\.spinetoolbox\\items\\helloworld\\output\\2022-08-19T13.03.13' href='file:///C:\\data\\GIT\\SPINEENGINE\\spine_engine\\server\\received_projects\\helloworld__35bc62cea0324e8788144ce81342f4f1\\.spinetoolbox\\items\\helloworld\\output\\2022-08-19T13.03.13'>results directory</a> ***",
                },
            ),
            (
                'exec_finished',
                {
                    'item_name': 'helloworld',
                    'direction': 'FORWARD',
                    'state': 'RUNNING',
                    'item_state': ItemExecutionFinishState.SUCCESS,
                },
            ),
            ('dag_exec_finished', 'COMPLETED'),
        ]
        return test_events

    def test_convert(self):
        event_data = self.make_event_data()
        converted_events = list()
        for event in event_data:
            json_str = EventDataConverter.convert(event[0], event[1])
            converted_events.append(json_str)
        self.assertEqual(16, len(converted_events))
        # Check that item_state values are cast to strings i.e. ItemExecutionFinishState.SUCCESS -> "SUCCESS"
        n = 0  # Counter for how many ItemExecutionFinishStates were cast to strings
        for converted_event in converted_events:
            if converted_event.startswith('{"event_type": "exec_finished"'):
                self.assertTrue('"item_state": "SUCCESS"' in converted_event)
                n += 1
        self.assertEqual(4, n)

    def test_convert_deconvert(self):
        """Converts events, then deconverts them back."""
        event_data = self.make_event_data()
        expected_data = deepcopy(event_data)
        converted_events = list()
        for event in event_data:
            json_str = EventDataConverter.convert(event[0], event[1])
            converted_events.append(json_str)
        self.assertEqual(16, len(converted_events))
        deconverted_events = list()
        for conv_event in converted_events:
            event_tuple = EventDataConverter.deconvert(conv_event.encode("utf-8"))
            deconverted_events.append(event_tuple)
        self.assertEqual(16, len(deconverted_events))
        # The converted & deconverted list must be equal to the original
        self.assertEqual(expected_data, deconverted_events)


if __name__ == '__main__':
    unittest.main()
