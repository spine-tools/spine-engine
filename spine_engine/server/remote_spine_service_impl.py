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

from spine_engine import SpineEngine


class RemoteSpineServiceImpl:
    """A service implementation for execution of DAGs with Spine Engine."""
    @staticmethod
    def execute(data):
        """
        Execute a DAG with Spine Engine based on the provided data.
        A prerequisite is that associated data files(project directory) has been saved into a respective 
        folder (referred to by the provided data).

        Args:
            data (dict): Contains data to be provided to the spine_engine (see SpineEnegine.__init_())

        Returns:
            list: A list of tuples containing event and data returned from the SpineEngine.
        """
        engine = SpineEngine(
            items=data["items"],
            specifications=data["specifications"],
            connections=data["connections"],
            jumps=data["jumps"],
            node_successors=data["node_successors"],
            execution_permits=data["execution_permits"],
            items_module_name=data["items_module_name"],
            settings=data["settings"],
            project_dir=data["project_dir"],
        )
        # get events+data from the spine engine
        event_data = []
        while True:
            event_type, data = engine.get_event()
            event_data.append((event_type, data))
            if data == "COMPLETED" or data == "FAILED":
                return event_data
