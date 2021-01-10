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

from enum import Enum, auto
import datetime
import threading
import multiprocessing as mp
from itertools import product
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
from spinedb_api import append_filter_config, name_from_dict
from spinedb_api.filters.scenario_filter import scenario_name_from_dict
from spinedb_api.filters.execution_filter import execution_filter_config
from .utils.helpers import AppSettings, inverted
from .utils.queue_logger import QueueLogger
from .load_project_items import ProjectItemLoader
from .multithread_executor.executor import multithread_executor


class ExecutionDirection(Enum):
    FORWARD = auto()
    BACKWARD = auto()

    def __str__(self):
        return str(self.name)


ED = ExecutionDirection


class SpineEngineState(Enum):
    SLEEPING = 1
    RUNNING = 2
    USER_STOPPED = 3
    FAILED = 4
    COMPLETED = 5

    def __str__(self):
        return str(self.name)


class SpineEngine:
    """
    An engine for executing a Spine Toolbox DAG-workflow.
    """

    def __init__(
        self,
        items=None,
        specifications=None,
        filter_stacks=None,
        settings=None,
        project_dir=None,
        execution_permits=None,
        node_successors=None,
        debug=True,
    ):
        """
        Inits class.

        Args:
            items (list(dict)): List of executable item dicts.
            specifications (dict(str,list(dict))): A mapping from item type to list of specification dicts.
            filter_stacks (dict): A mapping from tuple (resource label, receiving item name) to a list of applicable filters.
            settings (dict): Toolbox execution settings.
            project_dir (str): Path to project directory.
            execution_permits (dict(str,bool)): A mapping from item name to a boolean value, False indicating that
                the item is not executed, only its resources are collected.
            node_successors (dict(str,list(str))): A mapping from item name to list of successor item names, dictating the dependencies.
            debug (bool): Whether debug mode is active or not.
        """
        super().__init__()
        self._queue = mp.Queue()
        if items is None:
            items = []
        self._items = items
        if filter_stacks is None:
            filter_stacks = {}
        self._filter_stacks = filter_stacks
        self._settings = AppSettings(settings)
        self._project_dir = project_dir
        project_item_loader = ProjectItemLoader()
        self._executable_item_classes = project_item_loader.load_executable_item_classes()
        if specifications is None:
            specifications = {}
        self._item_specifications = self._make_item_specifications(specifications, project_item_loader)
        self._solid_names = {item_name: str(i) for i, item_name in enumerate(items)}
        self._item_names = {solid_name: item_name for item_name, solid_name in self._solid_names.items()}
        if execution_permits is None:
            execution_permits = {}
        self._execution_permits = {self._solid_names[name]: permits for name, permits in execution_permits.items()}
        if node_successors is None:
            node_successors = {}
        self._back_injectors = {
            self._solid_names[key]: [self._solid_names[x] for x in value] for key, value in node_successors.items()
        }
        self._forth_injectors = inverted(self._back_injectors)
        self._pipeline = self._make_pipeline()
        self._state = SpineEngineState.SLEEPING
        self._debug = debug
        self._running_items = []
        self._event_stream = self._get_event_stream()

    def _make_item_specifications(self, specifications, project_item_loader):
        specification_factories = project_item_loader.load_item_specification_factories()
        item_specifications = {}
        for item_type, spec_dicts in specifications.items():
            factory = specification_factories.get(item_type)
            if factory is None:
                continue
            item_specifications[item_type] = dict()
            for spec_dict in spec_dicts:
                spec = factory.make_specification(spec_dict, self._settings, None)
                item_specifications[item_type][spec.name] = spec
        return item_specifications

    def _make_item(self, item_name):
        item_dict = self._items[item_name]
        item_type = item_dict["type"]
        executable_item_class = self._executable_item_classes[item_type]
        logger = QueueLogger(self._queue, item_name)
        return executable_item_class.from_dict(
            item_dict, item_name, self._project_dir, self._settings, self._item_specifications, logger
        )

    def get_event(self):
        """Returns the next event in the stream. Calling this after receiving the event of type "dag_exec_finished"
        will raise StopIterationError.
        """
        return next(self._event_stream)

    def state(self):
        return self._state

    def _get_event_stream(self):
        """Returns an iterator of tuples (event_type, event_data).

        TODO: Describe the events in depth.
        """
        threading.Thread(target=self.run).start()
        while True:
            msg = self._queue.get()
            yield msg
            if msg[0] == "dag_exec_finished":
                break

    def run(self):
        """Runs this engine."""
        self._state = SpineEngineState.RUNNING
        run_config = {"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}, "execution": {"multithread": {}}}
        for event in execute_pipeline_iterator(self._pipeline, run_config=run_config):
            self._process_event(event)
        if self._state == SpineEngineState.RUNNING:
            self._state = SpineEngineState.COMPLETED
        self._queue.put(("dag_exec_finished", str(self._state)))

    def _process_event(self, event):
        """
        Processes events from a pipeline.

        Args:
            event (DagsterEvent): an event
        """
        if event.event_type == DagsterEventType.STEP_START:
            direction, _, solid_name = event.solid_name.partition("_")
            item_name = self._item_names[solid_name]
            self._queue.put(('exec_started', {"item_name": item_name, "direction": direction}))
        elif event.event_type == DagsterEventType.STEP_FAILURE and self._state != SpineEngineState.USER_STOPPED:
            direction, _, solid_name = event.solid_name.partition("_")
            item_name = self._item_names[solid_name]
            self._state = SpineEngineState.FAILED
            self._queue.put(
                (
                    'exec_finished',
                    {
                        "item_name": item_name,
                        "direction": direction,
                        "state": str(self._state),
                        "success": False,
                        "skipped": False,
                    },
                )
            )
            if self._debug:
                error = event.event_specific_data.error
                print("Traceback (most recent call last):")
                print("".join(error.stack + [error.message]))
                print("(reported by SpineEngine in debug mode)")
        elif event.event_type == DagsterEventType.STEP_SUCCESS:
            direction, _, solid_name = event.solid_name.partition("_")
            item_name = self._item_names[solid_name]
            self._queue.put(
                (
                    'exec_finished',
                    {
                        "item_name": item_name,
                        "direction": direction,
                        "state": str(self._state),
                        "success": True,
                        "skipped": not self._execution_permits[solid_name],
                    },
                )
            )

    def stop(self):
        """Stops this engine.
        """
        self._state = SpineEngineState.USER_STOPPED
        for item in self._running_items:
            item.stop_execution()
            self._queue.put(
                (
                    'exec_finished',
                    {
                        "item_name": item.name,
                        "direction": str(ED.FORWARD),
                        "state": str(self._state),
                        "success": False,
                        "skipped": not self._execution_permits[self._solid_names[item.name]],
                    },
                )
            )
        self._queue.put(("dag_exec_finished", str(self._state)))

    def _make_pipeline(self):
        """
        Returns a PipelineDefinition for executing this engine.

        Returns:
            PipelineDefinition
        """
        solid_defs = [
            make_solid_def(item_name)
            for item_name in self._items
            for make_solid_def in (self._make_forward_solid_def, self._make_backward_solid_def)
        ]
        dependencies = self._make_dependencies()
        mode_defs = [ModeDefinition(executor_defs=default_executors + [multithread_executor])]
        return PipelineDefinition(
            name="pipeline", solid_defs=solid_defs, dependencies=dependencies, mode_defs=mode_defs
        )

    def _make_backward_solid_def(self, item_name):
        """Returns a SolidDefinition for executing the given item in the backward sweep.

        Args:
            item_name (str): The project item that gets executed by the solid.
        """

        def compute_fn(context, inputs):
            if self.state() == SpineEngineState.USER_STOPPED:
                context.log.error(f"compute_fn() FAILURE with item: {item_name} stopped by the user")
                raise Failure()
            context.log.info(f"Item Name: {item_name}")
            item = self._make_item(item_name)
            resources = item.output_resources(ED.BACKWARD)
            yield Output(value=resources, output_name=f"{ED.BACKWARD}_output")

        input_defs = []
        output_defs = [OutputDefinition(name=f"{ED.BACKWARD}_output")]
        return SolidDefinition(
            name=f"{ED.BACKWARD}_{self._solid_names[item_name]}",
            input_defs=input_defs,
            compute_fn=compute_fn,
            output_defs=output_defs,
        )

    def _make_forward_solid_def(self, item_name):
        """Returns a SolidDefinition for executing the given item.

        Args:
            item_name (str)

        Returns:
            SolidDefinition
        """

        def compute_fn(context, inputs):
            if self.state() == SpineEngineState.USER_STOPPED:
                context.log.error(f"compute_fn() FAILURE with item: {item_name} stopped by the user")
                raise Failure()
            context.log.info(f"Item Name: {item_name}")
            # Split inputs into forward and backward resources based on prefix
            forward_resource_stacks = []
            backward_resources = []
            for name, values in inputs.items():
                if name.startswith(f"{ED.FORWARD}"):
                    forward_resource_stacks += values
                elif name.startswith(f"{ED.BACKWARD}"):
                    backward_resources += values
            output_resource_stacks = self._execute_item(context, item_name, forward_resource_stacks, backward_resources)
            yield Output(value=output_resource_stacks, output_name=f"{ED.FORWARD}_output")

        input_defs = [
            InputDefinition(name=f"{ED.FORWARD}_input_from_{inj}")
            for inj in self._forth_injectors.get(self._solid_names[item_name], [])
        ] + [
            InputDefinition(name=f"{ED.BACKWARD}_input_from_{inj}")
            for inj in self._back_injectors.get(self._solid_names[item_name], [])
        ]
        output_defs = [OutputDefinition(name=f"{ED.FORWARD}_output")]
        return SolidDefinition(
            name=f"{ED.FORWARD}_{self._solid_names[item_name]}",
            input_defs=input_defs,
            compute_fn=compute_fn,
            output_defs=output_defs,
        )

    def _execute_item(self, context, item_name, forward_resource_stacks, backward_resources):
        """Executes the given item using the given forward resource stacks and backward resources.
        Returns list of output resource stacks.

        Called by ``_make_forward_solid_def.compute_fn``.

        For each element yielded by ``_filtered_resources_iterator``, spawns a thread that runs ``_execute_item_filtered``.

        Args:
            context
            item_name (str)
            forward_resource_stacks (list(tuple(ProjectItemResource)))
            backward_resources (list(ProjectItemResource))

        Returns:
            list(tuple(ProjectItemResource))
        """
        success = [True]
        output_resources_list = []
        threads = []
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        resources_iterator = self._filtered_resources_iterator(
            item_name, forward_resource_stacks, backward_resources, timestamp
        )
        for flt_fwd_resources, flt_bwd_resources, filter_id in resources_iterator:
            item = self._make_item(item_name)
            item.filter_id = filter_id
            thread = threading.Thread(
                target=self._execute_item_filtered,
                args=(item, flt_fwd_resources, flt_bwd_resources, output_resources_list, success),
            )
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        if not success[0]:
            context.log.error(f"compute_fn() FAILURE with item: {item_name} failed to execute")
            raise Failure()
        return list(zip(*output_resources_list))

    def _execute_item_filtered(
        self, item, filtered_forward_resources, filtered_backward_resources, output_resources_list, success
    ):
        """Executes the given item using the given filtered resources. Target for threads in ``_execute_item``.

        Args:
            item (ExecutableItemBase)
            filtered_forward_resources (list(ProjectItemResource))
            filtered_backward_resources (list(ProjectItemResource))
            output_resources_list (list(list(ProjectItemResource))): A list of lists, to append the
                output resources generated by the item.
            success (list): A list of one element, to write the outcome of the execution.

        """
        self._running_items.append(item)
        if self._execution_permits[self._solid_names[item.name]]:
            item_success = item.execute(filtered_forward_resources, filtered_backward_resources)
            item.finish_execution(item_success)
        else:
            item.skip_execution(filtered_forward_resources, filtered_backward_resources)
            item_success = True
        filter_stack = sum((r.metadata.get("filter_stack", ()) for r in filtered_forward_resources), ())
        output_resources = item.output_resources(ED.FORWARD)
        for resource in output_resources:
            resource.metadata["filter_stack"] = filter_stack
            resource.metadata["filter_id"] = item.filter_id
        output_resources_list.append(output_resources)
        success[0] &= item_success  # FIXME: We need a Lock here
        self._running_items.remove(item)

    def _filtered_resources_iterator(self, item_name, forward_resource_stacks, backward_resources, timestamp):
        """Yields tuples of (filtered forward resources, filtered backward resources, filter id).

        Each tuple corresponds to a unique filter combination. Combinations are obtained by applying the cross-product
        over forward resource stacks as yielded by ``_forward_resource_stacks_iterator``.

        Args:
            item_name (str)
            forward_resource_stacks (list(tuple(ProjectItemResource)))
            backward_resources (list(ProjectItemResource))
            timestamp (str): timestamp for the execution filter

        Returns:
            Iterator(tuple(list,list,str)): forward resources, backward resources, filter id
        """

        def check_resource_affinity(filtered_forward_resources):
            filter_ids_by_provider = dict()
            for r in filtered_forward_resources:
                filter_ids_by_provider.setdefault(r.provider.name, set()).add(r.metadata.get("filter_id"))
            return all(len(filter_ids) == 1 for filter_ids in filter_ids_by_provider.values())

        forward_resource_stacks_iterator = (
            self._expand_resource_stack(item_name, resource_stack) for resource_stack in forward_resource_stacks
        )
        for filtered_forward_resources in product(*forward_resource_stacks_iterator):
            if not check_resource_affinity(filtered_forward_resources):
                continue
            resource_filter_stack = {r.label: r.metadata.get("filter_stack", ()) for r in filtered_forward_resources}
            scenarios = {scenario_name_from_dict(cfg) for stack in resource_filter_stack.values() for cfg in stack}
            scenarios.discard(None)
            execution = {"execution_item": item_name, "scenarios": list(scenarios), "timestamp": timestamp}
            config = execution_filter_config(execution)
            filtered_backward_resources = []
            for resource in backward_resources:
                clone = resource.clone(additional_metadata={"label": resource.label})
                clone.url = append_filter_config(clone.url, config)
                filtered_backward_resources.append(clone)
            filter_id = _make_filter_id(resource_filter_stack)
            yield list(filtered_forward_resources), filtered_backward_resources, filter_id

    def _expand_resource_stack(self, item_name, resource_stack):
        """Expands a resource stack if possible.

        If the stack has more than one resource, returns the unaltered stack.

        Otherwise, if the stack has only one resource but there are no filters defined for that resource,
        again, returns the unaltered stack.

        Otherwise, returns an expanded stack of as many resources as filter stacks defined for the only one resource.
        Each resource in the expanded stack is a clone of the original, with one of the filter stacks
        applied to the URL.

        Args:
            item_name (str)
            resource_stack (tuple(ProjectItemResource))

        Returns:
            tuple(ProjectItemResource)
        """
        if len(resource_stack) > 1:
            return resource_stack
        resource = resource_stack[0]
        filter_stacks = self._filter_stacks.get((resource.label, item_name))
        if not filter_stacks:
            return resource_stack
        expanded_stack = ()
        for filter_stack in filter_stacks:
            filtered_clone = resource.clone(additional_metadata={"label": resource.label, "filter_stack": filter_stack})
            for config in filter_stack:
                if config:
                    filtered_clone.url = append_filter_config(filtered_clone.url, config)
            expanded_stack += (filtered_clone,)
        return expanded_stack

    def _make_dependencies(self):
        """
        Returns a dictionary of dependencies according to the given dictionaries of injectors.

        Returns:
            dict: a dictionary to pass to the PipelineDefinition constructor as dependencies
        """
        forward_deps = {
            f"{ED.FORWARD}_{n}": {
                f"{ED.FORWARD}_input_from_{inj}": DependencyDefinition(f"{ED.FORWARD}_{inj}", f"{ED.FORWARD}_output")
                for inj in injs
            }
            for n, injs in self._forth_injectors.items()
        }
        backward_deps = {
            f"{ED.FORWARD}_{n}": {
                f"{ED.BACKWARD}_input_from_{inj}": DependencyDefinition(f"{ED.BACKWARD}_{inj}", f"{ED.BACKWARD}_output")
                for inj in injs
            }
            for n, injs in self._back_injectors.items()
        }
        deps = {}
        for n in forward_deps.keys() | backward_deps.keys():
            deps[n] = forward_deps.get(n, {})
            deps[n].update(backward_deps.get(n, {}))
        return deps


def _make_filter_id(resource_filter_stack):
    resources = []
    for resource, stack in resource_filter_stack.items():
        names = "&".join(_filter_names_from_stack(stack))
        if names:
            resource += f" with {names}"
        resources.append(resource)
    return ", ".join(resources)


def _filter_names_from_stack(stack):
    for config in stack:
        if not config:
            continue
        filter_name = name_from_dict(config)
        if filter_name is not None:
            yield filter_name
