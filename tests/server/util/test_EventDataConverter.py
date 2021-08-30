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
:author: P. Pääkkönen (VTT)
:date:   27.8.2021
"""

import unittest
import json
import sys
sys.path.append('./../../../spine_engine/server/util')
import os

from EventDataConverter import EventDataConverter

class TestEventDataConverter(unittest.TestCase):


    def _createEventData(self):
        eventData=[]
        i=0
        while i< 20:
            event='exec_finished'
            data="'item_name': 'helloworld','direction': 'BACKWARD','state': 'RUNNING','item_state': < ItemExecutionFinishState.SUCCESS: 1 >"
            eventData.append((event,data))
            i+=1
        return eventData

    def test_converting_basic(self):
        eventData=self._createEventData()
        jsonStr=EventDataConverter.convert(eventData)
        #print(jsonStr)
        json.loads(jsonStr)

    def test_2converts_basic(self):
        eventData=self._createEventData()
        jsonStr=EventDataConverter.convert(eventData)
        eventsData2=EventDataConverter.convertJSON(jsonStr,True)
        #print(eventsData2)
        jsonStr2=EventDataConverter.convert(eventsData2)
        self.assertEqual(jsonStr,jsonStr2)
        

if __name__ == '__main__':
    unittest.main()

