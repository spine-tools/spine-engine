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
Contains the RemoteSpineServiceImpl class for running a DAG with Spine Engine.
Only one DAG can be executed at a time.
:authors: P. Pääkkönen (VTT)
:date:   18.08.2021
"""

from enum import unique, Enum

import sys
#sys.path.append('./../../spine_engine')
from spine_engine import SpineEngine

#@unique
#class RemoteSpineServiceImplState(Enum):
#    IDLE = 1
#    RUNNING = 2



class RemoteSpineServiceImpl:
    """
    A service implementation for execution of DAGs with Spine Engine. 
    """


    def __init__(
        self
    ):
        """
        Args:
            None
        """
        #self._state=RemoteSpineServiceImplState.IDLE

    def execute(self,data):
        """
        Execute a DAG with Spine Engine based on the provided data.
        A prerequisite is that associated data files(project directory) has been saved into a respective 
        folder (referred to by the provided data).
        Args:
            data (dict): Contains data to be provided to the spine_engine (see SpineEnegine.__init_())
        Returns:
            A list of tuples containing event and data returned from the SpineEngine.
        """

        #check for correct input 
        if data['items']==None or data['connections']==None or data['node_successors']==None or data['execution_permits']==None or\
        data['specifications']==None or data['settings']==None or data['project_dir']==None or\
        data['items']=="" or data['connections']=="" or data['node_successors']=="" or data['execution_permits']=="" or\
        data['specifications']=="" or data['settings']=="" or data['project_dir']=="":
            raise ValueError("invalid data content provided to RemoteSpineServiceImpl.execute()")

        #check our state
        #if self._state==RemoteSpineServiceImplState.IDLE:
        #    self._state=RemoteSpineServiceImplState.RUNNING

        #print("RemoteSpineServiceImpl.execute() calling spine_engine with data %s"%data)
        engine = SpineEngine(items=data['items'], specifications=data['specifications'],connections=data['connections'],jumps=data['jumps'], node_successors=data['node_successors'],
            execution_permits=data['execution_permits'],items_module_name=data['items_module_name'],settings=data['settings'],
            project_dir=data['project_dir'])

        #engine.run()
        #get events+data from the spine engine
        eventData=[]
        while True:
            event_type, data = engine.get_event()
            eventData.append((event_type,data))
            #print("RemoteSpineServiceImpl.execute() event type: %s"%type(event_type))
            #print("RemoteSpineServiceImpl.execute() data type: %s"%type(data))
            #print("RemoteSpineServiceImpl.execute() data: %s"%data)
            if data == "COMPLETED" or data =="FAILED":
                #print("finished, data:")
                #print(data)
                #print("engine state:")
                #print(engine.state())
                #self._state=RemoteSpineServiceImplState.IDLE
                return eventData


