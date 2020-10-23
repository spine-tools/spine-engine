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

import json
import threading
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
    DagsterInstance,
    EventMetadataEntry,
    default_executors,
)
from dagster_celery import celery_executor
from dagster.core.definitions.reconstructable import build_reconstructable_pipeline
from spine_engine.event_publisher import EventPublisher
from .helpers import (
    AppSettings,
    inverted,
    Messenger,
    ItemExecutionStart,
    ItemExecutionFinish,
)
from .load_project_items import ProjectItemLoader
from .spine_engine import ExecutionDirection, SpineEngineState


def _make_executable_items(items, specifications, settings, project_dir, logger):
    project_item_loader = ProjectItemLoader()
    specification_factories = project_item_loader.load_item_specification_factories()
    executable_item_classes = project_item_loader.load_executable_item_classes()
    app_settings = AppSettings(settings)
    item_specifications = {}
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
        item = executable_item_class.from_dict(
            item_dict, item_name, project_dir, app_settings, item_specifications, logger
        )
        executable_items.append(item)
    return executable_items


def make_pipeline(json_data):
    """
    Returns a PipelineDefinition for executing the given items in the given direction,
    generating dependencies from the given injectors.

    Args:
        engine_id (int)

    Returns:
        PipelineDefinition
    """
    data = json.loads(json_data)
    items = data["items"]
    specifications = data["specifications"]
    settings = data["settings"]
    project_dir = data["project_dir"]
    execution_permits = data["execution_permits"]
    successors = data["node_successors"]
    backward_injectors = successors
    forward_injectors = inverted(successors)
    messenger = Messenger()
    executable_items = _make_executable_items(items, specifications, settings, project_dir, messenger)
    solid_names = {item.name: str(i) for i, item in enumerate(executable_items)}
    solid_defs = [_make_backward_solid_def(item, solid_names) for item in executable_items]
    solid_defs += [
        _make_forward_solid_def(
            item, execution_permits[item.name], backward_injectors, forward_injectors, solid_names, messenger
        )
        for item in executable_items
    ]
    dependencies = _make_dependencies(backward_injectors, forward_injectors, solid_names)
    mode_defs = [ModeDefinition(executor_defs=default_executors + [celery_executor])]
    return PipelineDefinition(name="pipeline", solid_defs=solid_defs, mode_defs=mode_defs, dependencies=dependencies)


def _make_backward_solid_def(item, solid_names):
    """Returns a SolidDefinition for executing the given item backward.

    Args:
        item (ExecutableItemBase): The project item that gets executed by the solid.
        solid_names (dict): Mapping from item name to dagster-friendly solid name.

    Returns:
        SolidDefinition
    """
    backward = ExecutionDirection.BACKWARD

    def compute_fn(context, inputs):
        backward_resources = item.output_resources(backward)
        yield Output(value=backward_resources, output_name="result")
        yield Output(value=len(backward_resources), output_name="count")

    output_defs = [OutputDefinition(name="result"), OutputDefinition(name="count")]
    return SolidDefinition(
        name=_make_directed_solid_name(solid_names[item.name], backward),
        input_defs=[],
        compute_fn=compute_fn,
        output_defs=output_defs,
    )


def _make_forward_solid_def(item, execute, backward_injectors, forward_injectors, solid_names, messenger):
    """Returns a SolidDefinition for executing the given item forward.

    Args:
        item (ExecutableItemBase): The project item that gets executed by the solid.
        execute (bool): If False, do not execute the item, just collect resources.
        backward_injectors (dict): Mapping from item name to list of injector item names.
        forward_injectors (dict): Mapping from item name to list of injector item names.
        solid_names (dict): Mapping from item name to dagster-friendly solid name.
        messenger (Messenger)

    Returns:
        SolidDefinition
    """
    backward = ExecutionDirection.BACKWARD
    forward = ExecutionDirection.FORWARD

    def compute_fn(context, inputs):
        # Parse input into backward_resources and forward_resources, using the backward resource counts
        # Note that this depends on the input_defs being ordered of SolidDefinition
        input_values = list(inputs.values())
        backward_resource_first = next((k for k, x in enumerate(input_values) if isinstance(x, list)), 0)
        backward_resource_count = sum(input_values[:backward_resource_first])
        backward_resource_last = backward_resource_first + backward_resource_count
        backward_resources = [v for vals in input_values[backward_resource_first:backward_resource_last] for v in vals]
        forward_resources = [v for vals in input_values[backward_resource_last:] for v in vals]
        yield ItemExecutionStart("dummy", metadata_entries=[EventMetadataEntry.text(item.name, "")])
        if execute:
            # Do the work in a different thread

            def worker():
                success = item.execute(backward_resources, backward)
                success &= item.execute(forward_resources, forward)
                messenger.close(success)

            threading.Thread(target=worker).start()
            # Do the control in the current thread
            while True:
                if messenger.is_closed():
                    break
                msg = messenger.get_msg()
                yield msg.materialization()
                messenger.task_done()
        yield ItemExecutionFinish("dummy", metadata_entries=[EventMetadataEntry.text(item.name, "")])
        if messenger.success() is False:
            raise Failure()
        yield Output(value=item.output_resources(forward), output_name="result")

    # Input defs are ordered as follows: first we have the backward resource counts,
    # then backward resources, and finally foward resources.
    # This is so our parsing routine in the compute function works
    input_defs = (
        [
            InputDefinition(name=_make_directed_input_name(solid_names[n], backward) + "_count")
            for n in backward_injectors.get(item.name, [])
        ]
        + [
            InputDefinition(name=_make_directed_input_name(solid_names[n], backward))
            for n in backward_injectors.get(item.name, [])
        ]
        + [
            InputDefinition(name=_make_directed_input_name(solid_names[n], forward))
            for n in forward_injectors.get(item.name, [])
        ]
    )
    output_defs = [OutputDefinition(name="result")]
    return SolidDefinition(
        name=_make_directed_solid_name(solid_names[item.name], forward),
        input_defs=input_defs,
        compute_fn=compute_fn,
        output_defs=output_defs,
    )


def _make_dependencies(backward_injectors, forward_injectors, solid_names):
    """
    Returns a dictionary of dependencies based on the given dictionaries of injectors.

    Args:
        backward_injectors (dict): Mapping from item name to list of injector item names.
        forward_injectors (dict): Mapping from item name to list of injector item names.
        solid_names (dict): Mapping from item name to dagstter-friendly item name.

    Returns:
        dict: a dictionary to pass to the PipelineDefinition constructor as dependencies
    """
    dependencies = {}
    for item_name, solid_name in solid_names.items():
        forward_solid_name = _make_directed_solid_name(solid_name, ExecutionDirection.FORWARD)
        forward_solid_deps = dependencies[forward_solid_name] = dict()
        for backward_injector_name in backward_injectors.get(item_name, []):
            bakward_injector_solid_name = _make_directed_solid_name(
                solid_names[backward_injector_name], ExecutionDirection.BACKWARD
            )
            backward_input_name = _make_input_name(bakward_injector_solid_name)
            # Forward solid depends on the backward input
            forward_solid_deps[backward_input_name] = DependencyDefinition(bakward_injector_solid_name, "result")
            forward_solid_deps[backward_input_name + "_count"] = DependencyDefinition(
                bakward_injector_solid_name, "count"
            )
        for forward_injector_name in forward_injectors.get(item_name, []):
            forward_injector_solid_name = _make_directed_solid_name(
                solid_names[forward_injector_name], ExecutionDirection.FORWARD
            )
            forward_input_name = _make_input_name(forward_injector_solid_name)
            # Forward solid depends on the forward input
            forward_solid_deps[forward_input_name] = DependencyDefinition(forward_injector_solid_name, "result")
    return dependencies


def _make_directed_solid_name(solid_name, direction):
    return f"{solid_name}_{direction}"


def _make_input_name(solid_name):
    return f"input_from_{solid_name}"


def _make_directed_input_name(solid_name, direction):
    return _make_input_name(_make_directed_solid_name(solid_name, direction))


class SpineEngineExperimental:
    """
    An engine for executing a Spine Toolbox DAG-workflow.

    The engine consists of two pipelines:
    - One backwards, where ProjectItems collect resources from successor items if applies
    - One forward, where actual execution happens.
    """

    def __init__(self, json_data, debug=True, use_multiprocess_execution=False):
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
            use_multiprocess_execution (bool): Whether or not to use multiprocess execution
        """
        super().__init__()
        self._json_data = json_data
        self._publisher = EventPublisher()
        self._state = SpineEngineState.SLEEPING
        self._debug = debug
        self._use_multiprocess_execution = use_multiprocess_execution

    @property
    def publisher(self):
        return self._publisher

    def state(self):
        return self._state

    def run(self):
        if self._use_multiprocess_execution:
            self._run_multiprocess()
        else:
            self._run_singleprocess()

    def _run_multiprocess(self):
        """Runs this engine.
        """
        self._state = SpineEngineState.RUNNING
        run_config = {
            # "execution": {"celery": {}},
            "execution": {"multiprocess": {}},
            "storage": {"filesystem": {}},
            "loggers": {"console": {"config": {"log_level": "CRITICAL"}}},
        }
        instance = DagsterInstance.local_temp()
        pipeline = build_reconstructable_pipeline(
            "spine_engine.spine_engine_experimental", "make_pipeline", (self._json_data,),
        )
        for event in execute_pipeline_iterator(pipeline, run_config=run_config, instance=instance):
            self._process_event(event)
            if self.state() == SpineEngineState.USER_STOPPED:
                break
        if self._state == SpineEngineState.RUNNING:
            self._state = SpineEngineState.COMPLETED

    def _run_singleprocess(self):
        """Runs this engine.
        """
        self._state = SpineEngineState.RUNNING
        run_config = {"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}}
        pipeline = make_pipeline(self._json_data)
        for event in execute_pipeline_iterator(pipeline, run_config=run_config):
            self._process_event(event)
            if self.state() == SpineEngineState.USER_STOPPED:
                break
        if self._state == SpineEngineState.RUNNING:
            self._state = SpineEngineState.COMPLETED

    def stop(self):
        """Stops this engine.
        """
        self._state = SpineEngineState.USER_STOPPED

    def _process_event(self, event):
        """
        Processes events from a pipeline.

        Args:
            event (DagsterEvent): an event
            direction (ExecutionDirection): execution direction
        """
        if event.event_type == DagsterEventType.STEP_MATERIALIZATION:
            event.event_specific_data.materialization.dispatch_event(self)
        elif event.event_type == DagsterEventType.STEP_FAILURE:
            if self._state != SpineEngineState.USER_STOPPED:
                self._state = SpineEngineState.FAILED
            if self._debug:
                error = event.event_specific_data.error
                print("Traceback (most recent call last):")
                print("".join(error.stack + [error.message]))
                print("(generated by SpineEngineExperimental in debug mode)")
