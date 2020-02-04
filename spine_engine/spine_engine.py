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
Contains the SpineEngine class for running Spine Toolbox DAGs.

:authors: M. Marin (KTH)
:date:   20.11.2019
"""

from enum import auto, Enum
from dagster import (
    PipelineDefinition,
    SolidDefinition,
    InputDefinition,
    OutputDefinition,
    DependencyDefinition,
    Output,
    Failure,
    execute_pipeline_iterator,
    DagsterEventType,
)
from dagster.core.definitions.utils import DISALLOWED_NAMES, has_valid_name_chars
from PySide2.QtCore import QObject, Signal


def _inverted(input_):
    """Inverts a dictionary of list values.

    Args:
        input_ (dict)

    Returns:
        dict: keys are list items, and values are keys listing that item from the input dictionary
    """
    output = dict()
    for key, value_list in input_.items():
        for value in value_list:
            output.setdefault(value, list()).append(key)
    return output


class SpineEngineState(Enum):
    SLEEPING = 1
    RUNNING = 2
    USER_STOPPED = 3
    FAILED = 4
    COMPLETED = 5


class ExecutionDirection(Enum):
    FORWARD = auto()
    BACKWARD = auto()


class SpineEngine(QObject):
    """
    An engine for executing a Spine Toolbox DAG-workflow.

    The engine consists of two pipelines:
    - One backwards, where ProjectItems collect resources from successor items if applies
    - One forward, where actual execution happens.
    """

    dag_node_execution_started = Signal(str, "QVariant")
    """Emitted just before a named DAG node execution starts."""
    dag_node_execution_finished = Signal(str, "QVariant")
    """Emitted after a named DAG node has finished execution."""

    def __init__(self, project_items, successors, execution_permits):
        """
        Creates the two pipelines.

        Args:
            project_items (list(ProjectItem)): The items to execute.
            successors (dict): A mapping from item name to list of successor item names, dictating the dependencies.
            execution_permits (dict): A mapping from item name to a boolean value, False indicating that
                the item is not executed, only its resources are collected.
        """
        super().__init__()
        # Make lookup table for project item names to corresponding dagster friendly names (id's)
        self._name_lookup = self.make_name_lookup(project_items)
        # Make lookup table for dagster friendly names (id's) to corresponding ProjectItems
        self._project_item_lookup = self.make_project_item_lookup(project_items)
        back_injectors = {self._name_lookup[key]: [self._name_lookup[x] for x in value]
                          for key, value in successors.items()}
        forth_injectors = _inverted(back_injectors)
        # Change project item names in execution permits to corresponding id's
        fixed_exec_permits = self.fix_execution_permits(execution_permits)
        self._backward_pipeline = self._make_pipeline(project_items, back_injectors, "backward", fixed_exec_permits)
        self._forward_pipeline = self._make_pipeline(project_items, forth_injectors, "forward", fixed_exec_permits)
        self._state = SpineEngineState.SLEEPING
        self._running_item = None

    @staticmethod
    def make_name_lookup(project_items):
        """Returns a dictionary, where key is a project item 'long'
        name and value is a dagster friendly project item id.
        The id is just a rising integer number as a string. This
        is needed because we want to support executing project
        items with names that are disallowed in dagster and also
        project items with names that contain special characters
        that are allowed in Spine Toolbox but disallowed by dagster.

        Args:
            project_items (list(ProjectItem)): List of project items

        Returns:
            dict: Keys are project item names, values are integers as strings
        """
        return {item.name: str(i) for i, item in enumerate(project_items)}

    def make_project_item_lookup(self, project_items):
        """Returns a ProjectItem lookup table.

        Args:
            project_items (list(ProjectItem)): List of project items

        Returns:
            dict: Integer id's as keys, corresponding ProjectItem's as values
        """
        return {self._name_lookup[item.name]: item for item in project_items}

    def fix_execution_permits(self, execution_permits):
        """Returns a modified execution_permits table where keys
        have been replaced by a project item id (integer as str).

        Args:
            execution_permits (dict): Project item names as keys, booleans as values

        Returns:
            dict: Project item id's as keys, booleans as values
        """
        return {self._name_lookup[name]: execution_permits[name] for name in execution_permits.keys()}

    def state(self):
        return self._state

    @classmethod
    def from_cwl(cls, path):
        """Returns an instance of this class from a CWL file.

        Args:
            path (str): Path to a CWL file with the DAG-workflow description.

        Returns:
            SpineEngine
        """
        # TODO

    def run(self):
        """Runs this engine.
        """
        self._state = SpineEngineState.RUNNING
        environment_dict = {"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}}
        for event in execute_pipeline_iterator(self._backward_pipeline, environment_dict=environment_dict):
            self._process_event(event, ExecutionDirection.BACKWARD)
        for event in execute_pipeline_iterator(self._forward_pipeline, environment_dict=environment_dict):
            self._process_event(event, ExecutionDirection.FORWARD)
        if self._state == SpineEngineState.RUNNING:
            self._state = SpineEngineState.COMPLETED

    def stop(self):
        """Stops this engine.
        """
        self._state = SpineEngineState.USER_STOPPED
        if self._running_item:
            self._running_item.stop_execution()

    def _make_pipeline(self, project_items, injectors, direction, execution_permits):
        """
        Returns a PipelineDefinition for executing the given items in the given direction,
        generating dependencies from the given injectors.

        Args:
            project_items (list(ProjectItem)): List of project items for creating pipeline solids.
            injectors (dict(str,list(str))): A mapping from item name to list of injector item names.
            direction (str): The direction of the pipeline, either "forward" or "backward".
            execution_permits (dict): A mapping from item name to a boolean value, False indicating that
                the item is not executed, only its resources are collected.

        Returns:
            PipelineDefinition
        """
        solid_defs = [
            self._make_solid_def(item, injectors, direction, execution_permits[self._name_lookup[item.name]]) for item in project_items
        ]
        dependencies = self._make_dependencies(injectors)
        return PipelineDefinition(name=f"{direction}_pipeline", solid_defs=solid_defs, dependencies=dependencies)

    def _make_solid_def(self, item, injectors, direction, execute):
        """Returns a SolidDefinition for executing the given item in the given direction.

        Args:
            item (ProjectItem): The project item that gets executed by the solid.
            injectors (dict): Mapping from item name to list of injector item names.
            direction (str): The direction of execution, either "forward" or "backward".
            execute (bool): If False, do not execute the item, just collect resources.

        Returns:
            SolidDefinition
        """

        def compute_fn(context, inputs):
            if self.state() in (SpineEngineState.USER_STOPPED, SpineEngineState.FAILED):
                raise Failure()
            inputs = [val for values in inputs.values() for val in values]
            if execute and not item.execute(inputs, direction):
                raise Failure()
            yield Output(value=item.output_resources(direction), output_name="result")

        input_defs = [InputDefinition(name=f"input_from_{n}") for n in injectors.get(self._name_lookup[item.name], [])]
        output_defs = [OutputDefinition(name="result")]
        return SolidDefinition(
            name=self._name_lookup[item.name], input_defs=input_defs, compute_fn=compute_fn, output_defs=output_defs
        )

    @staticmethod
    def _make_dependencies(injectors):
        """
        Returns a dictionary of dependencies according to the given dictionary of injectors.

        Args:
            injectors (dict): Mapping from item name to list of injector item names.

        Returns:
            dict: a dictionary to pass to the PipelineDefinition constructor as dependencies
        """
        return {
            item_name: {f"input_from_{n}": DependencyDefinition(n, "result") for n in injector_names}
            for item_name, injector_names in injectors.items()
        }

    def _process_event(self, event, direction):
        """
        Processes events from a pipeline.

        Args:
            event (DagsterEvent): an event
            direction (ExecutionDirection): execution direction
        """
        if event.event_type == DagsterEventType.STEP_START:
            item = self._project_item_lookup[event.solid_name]
            self._running_item = item
            self.dag_node_execution_started.emit(item.name, direction)
        elif event.event_type == DagsterEventType.STEP_FAILURE:
            item = self._project_item_lookup[event.solid_name]
            self._running_item = item
            if self._state != SpineEngineState.USER_STOPPED:
                self._state = SpineEngineState.FAILED
            self.dag_node_execution_finished.emit(item.name, direction)
        elif event.event_type == DagsterEventType.STEP_SUCCESS:
            item = self._project_item_lookup[event.solid_name]
            self.dag_node_execution_finished.emit(item.name, direction)
