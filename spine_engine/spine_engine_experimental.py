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
Contains the SpineEngineExperimental class for running Spine Toolbox DAGs.

:authors: M. Marin (KTH)
:date:   20.11.2019
"""

import threading
import multiprocessing as mp
import json
from dagster import (
    PipelineDefinition,
    SolidDefinition,
    InputDefinition,
    OutputDefinition,
    DependencyDefinition,
    ModeDefinition,
    Output,
    Failure,
    execute_pipeline_iterator,
    DagsterEventType,
    default_executors,
)
from .utils.helpers import AppSettings, inverted
from .utils.queue_logger import QueueLogger
from .load_project_items import ProjectItemLoader
from .spine_engine import ExecutionDirection, SpineEngineState
from .multithread_executor.executor import multithread_executor


def _make_executable_items(items, specifications, settings, project_dir, master_queue):
    project_item_loader = ProjectItemLoader()
    specification_factories = project_item_loader.load_item_specification_factories()
    executable_item_classes = project_item_loader.load_executable_item_classes()
    app_settings = AppSettings(settings)
    item_specifications = {}
    logger = QueueLogger(master_queue)
    for item_type, spec_dicts in specifications.items():
        factory = specification_factories.get(item_type)
        if factory is None:
            continue
        item_specifications[item_type] = dict()
        for spec_dict in spec_dicts:
            spec = factory.make_specification(spec_dict, app_settings, logger)
            item_specifications[item_type][spec.name] = spec
    executable_items = []
    for item_name, item_dict in items.items():
        item_type = item_dict["type"]
        executable_item_class = executable_item_classes[item_type]
        logger = QueueLogger(master_queue, author=item_name)
        item = executable_item_class.from_dict(
            item_dict, item_name, project_dir, app_settings, item_specifications, logger
        )
        executable_items.append(item)
    return executable_items


class SpineEngineExperimental:
    """
    An engine for executing a Spine Toolbox DAG-workflow.

    The engine consists of two pipelines:
    - One backwards, where ProjectItems collect resources from successor items if applies
    - One forward, where actual execution happens.
    """

    _DONE = "DONE"

    def __init__(self, json_data, debug=True):
        """
        Inits class.

        Args:
            items (list(dict)): List of executable item dicts.
            specifications (dict(str,list(dict))): A mapping from item type to list of specification dicts.
            settings (dict): Toolbox execution settings.
            project_dir (str): Path to project directory.
            execution_permits (dict(str,bool)): A mapping from item name to a boolean value, False indicating that
                the item is not executed, only its resources are collected.
            successors (dict(str,list(str))): A mapping from item name to list of successor item names, dictating the dependencies.
            debug (bool): Whether debug mode is active or not.
        """
        super().__init__()
        self._queue = mp.Queue()
        self._state = SpineEngineState.SLEEPING
        self._debug = debug
        data = json.loads(json_data)
        items = data["items"]
        specifications = data["specifications"]
        settings = data["settings"]
        project_dir = data["project_dir"]
        execution_permits = data["execution_permits"]
        successors = data["node_successors"]
        executable_items = _make_executable_items(items, specifications, settings, project_dir, self._queue)
        self._solid_names = {item.name: str(i) for i, item in enumerate(executable_items)}
        self._executable_items = {self._solid_names[item.name]: item for item in executable_items}
        back_injectors = {
            self._solid_names[key]: [self._solid_names[x] for x in value] for key, value in successors.items()
        }
        forth_injectors = inverted(back_injectors)
        execution_permits = {self._solid_names[name]: permits for name, permits in execution_permits.items()}
        self._backward_pipeline = self._make_pipeline(
            executable_items, back_injectors, ExecutionDirection.BACKWARD, execution_permits
        )
        self._forward_pipeline = self._make_pipeline(
            executable_items, forth_injectors, ExecutionDirection.FORWARD, execution_permits
        )
        self._state = SpineEngineState.SLEEPING
        self._running_items = []

    @property
    def item_names(self):
        for item in self._executable_items.values():
            yield item.name

    def state(self):
        return self._state

    def run_iterator(self):
        threading.Thread(target=self.run).start()
        while True:
            msg = self._queue.get()
            if msg == self._DONE:
                break
            yield msg

    def run(self):
        """Runs this engine.
        """
        self._state = SpineEngineState.RUNNING
        run_config = {"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}}
        for event in execute_pipeline_iterator(self._backward_pipeline, run_config=run_config):
            self._process_event(event, ExecutionDirection.BACKWARD)
        run_config.update({"execution": {"multithread": {}}})
        for event in execute_pipeline_iterator(self._forward_pipeline, run_config=run_config):
            self._process_event(event, ExecutionDirection.FORWARD)
        if self._state == SpineEngineState.RUNNING:
            self._state = SpineEngineState.COMPLETED
        self._queue.put(self._DONE)

    def _process_event(self, event, direction):
        """
        Processes events from a pipeline.

        Args:
            event (DagsterEvent): an event
            direction (ExecutionDirection): execution direction
        """
        if event.event_type == DagsterEventType.STEP_START:
            item = self._executable_items[event.solid_name]
            self._running_items.append(item)
            self._queue.put(('exec_started', {"item_name": item.name, "direction": direction}))
        elif event.event_type == DagsterEventType.STEP_FAILURE:
            item = self._executable_items[event.solid_name]
            self._running_items.remove(item)
            if self._state != SpineEngineState.USER_STOPPED:
                self._state = SpineEngineState.FAILED
            self._queue.put(
                (
                    'exec_finished',
                    {"item_name": item.name, "direction": direction, "state": self._state, "success": False},
                )
            )
            if self._debug:
                error = event.event_specific_data.error
                print("Traceback (most recent call last):")
                print("".join(error.stack + [error.message]))
                print("(reported by SpineEngine in debug mode)")
        elif event.event_type == DagsterEventType.STEP_SUCCESS:
            item = self._executable_items[event.solid_name]
            self._running_items.remove(item)
            self._queue.put(
                (
                    'exec_finished',
                    {"item_name": item.name, "direction": direction, "state": self._state, "success": True},
                )
            )

    def stop(self):
        """Stops this engine.
        """
        self._state = SpineEngineState.USER_STOPPED
        for item in self._running_items:
            item.stop_execution()

    def _make_pipeline(self, executable_items, injectors, direction, execution_permits):
        """
        Returns a PipelineDefinition for executing the given items in the given direction,
        generating dependencies from the given injectors.

        Args:
            executable_items (list(ExecutableItemBase)): List of project items for creating pipeline solids.
            injectors (dict(str,list(str))): A mapping from item name to list of injector item names.
            direction (ExecutionDirection): The direction of the pipeline.
            execution_permits (dict): A mapping from item name to a boolean value, False indicating that
                the item is not executed, only its resources are collected.

        Returns:
            PipelineDefinition
        """
        solid_defs = [
            self._make_solid_def(item, injectors, direction, execution_permits[self._solid_names[item.name]])
            for item in executable_items
        ]
        dependencies = self._make_dependencies(injectors)
        mode_defs = [ModeDefinition(executor_defs=default_executors + [multithread_executor])]
        return PipelineDefinition(
            name=f"{direction}_pipeline", solid_defs=solid_defs, dependencies=dependencies, mode_defs=mode_defs
        )

    def _make_solid_def(self, item, injectors, direction, execute):
        """Returns a SolidDefinition for executing the given item in the given direction.

        Args:
            item (ExecutableItemBase): The project item that gets executed by the solid.
            injectors (dict): Mapping from item name to list of injector item names.
            direction (ExecutionDirection): The direction of execution.
            execute (bool): If False, do not execute the item, just collect resources.

        Returns:
            SolidDefinition
        """

        def compute_fn(context, inputs):
            if self.state() in (SpineEngineState.USER_STOPPED, SpineEngineState.FAILED):
                context.log.error(
                    "compute_fn() FAILURE with item: {0} is in state: {1}".format(item.name, self.state())
                )
                raise Failure()
            inputs = [val for values in inputs.values() for val in values]
            if execute:
                if not item.execute(inputs, direction):
                    context.log.error("compute_fn() FAILURE with item: {0} failed to execute".format(item.name))
                    raise Failure()
            else:
                item.skip_execution(inputs, direction)
            context.log.info("Item Name: {}".format(item.name))
            yield Output(value=item.output_resources(direction), output_name="result")

        input_defs = [InputDefinition(name=f"input_from_{n}") for n in injectors.get(self._solid_names[item.name], [])]
        output_defs = [OutputDefinition(name="result")]
        return SolidDefinition(
            name=self._solid_names[item.name], input_defs=input_defs, compute_fn=compute_fn, output_defs=output_defs
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
