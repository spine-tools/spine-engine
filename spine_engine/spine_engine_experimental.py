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
from spinedb_api import append_filter_config
from .utils.helpers import AppSettings, inverted
from .utils.queue_logger import QueueLogger
from .load_project_items import ProjectItemLoader
from .spine_engine import ExecutionDirection as ED, SpineEngineState
from .multithread_executor.executor import multithread_executor


class SpineEngineExperimental:
    """
    An engine for executing a Spine Toolbox DAG-workflow.

    The engine consists of two pipelines:
    - One backwards, where ProjectItems collect resources from successor items if applies
    - One forward, where actual execution happens.
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

    def _make_item(self, item_name, filter_id=""):
        item_dict = self._items[item_name]
        item_type = item_dict["type"]
        executable_item_class = self._executable_item_classes[item_type]
        logger = QueueLogger(self._queue, item_name, filter_id)
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
        """Returns a SolidDefinition for executing the given item in the forward sweep.

        Returns:
            SolidDefinition
        """

        def compute_fn(context, inputs):
            if self.state() == SpineEngineState.USER_STOPPED:
                context.log.error(f"compute_fn() FAILURE with item: {item_name} stopped by the user")
                raise Failure()
            context.log.info(f"Item Name: {item_name}")
            forward_resources = []
            backward_resources = []
            for name, values in inputs.items():
                if name.startswith(f"{ED.FORWARD}"):
                    forward_resources += values
                elif name.startswith(f"{ED.BACKWARD}"):
                    backward_resources += values
            if self._execution_permits[self._solid_names[item_name]]:

                def execute_item(item, success, forward_resources, backward_resources, output_resources):
                    self._running_items.append(item)
                    item_success = item.execute(forward_resources, backward_resources)
                    output_resources.update(item.output_resources(ED.FORWARD))
                    item.finish_execution(item_success)
                    success[0] &= item_success
                    self._running_items.remove(item)

                success = [True]
                output_resources = set()
                threads = []
                resources_iterator = self._filtered_resources_iterator(item_name, forward_resources, backward_resources)
                for forward_resources, backward_resources, filter_id in resources_iterator:
                    item = self._make_item(item_name, filter_id)
                    item.group_id = item_name + filter_id
                    thread = threading.Thread(
                        target=execute_item,
                        args=(item, success, forward_resources, backward_resources, output_resources),
                    )
                    threads.append(thread)
                    thread.start()
                for thread in threads:
                    thread.join()
                if not success[0]:
                    context.log.error(f"compute_fn() FAILURE with item: {item_name} failed to execute")
                    raise Failure()
                yield Output(value=list(output_resources), output_name=f"{ED.FORWARD}_output")
            else:
                item = self._make_item(item_name)
                item.skip_execution(forward_resources, backward_resources)
                resources = item.output_resources(ED.FORWARD)
                yield Output(value=resources, output_name=f"{ED.FORWARD}_output")

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

    def _filtered_resources_iterator(self, item_name, forward_resources, backward_resources):
        """Applies filter stacks defined for the given forward resources as they're being forwarded
        to given item, and yields tuples of filtered (forward, backward) resources

        For example, let's say we only have one forward resource. In this case,
        we apply the filter stack defined for the forward to the forward and to *all* the backward.

        Now let's say we have multiple forward resources. In this case, we first compute the cross product
        of filter stacks defined for all forwards. For each element in this product, we apply the corresponding
        stack to each forward, and the *union* of all stacks to each backward.


        Args:
            item_name (str)
            forward_resources (list(ProjectItemResource))
            backward_resources (list(ProjectItemResource))

        Returns:
            Iterator(tuple(list,list)): forward resources, backward resources
        """

        def filter_stacks_iterator():
            """Yields filter stacks for each forward resource, as it's being forwarded to given item."""
            for resource in forward_resources:
                key = (resource.label, item_name)
                yield self._filter_stacks.get(key, [{}])

        for stacks in product(*filter_stacks_iterator()):
            # Apply to each forward resource their own stack
            forward_clones = []
            for resource, stack in zip(forward_resources, stacks):
                clone = resource.clone(additional_metadata={"label": resource.label})
                for config in stack:
                    if config:
                        clone.url = append_filter_config(clone.url, config)
                forward_clones.append(clone)
            # Apply all stacks combined to each backward resource
            backward_clones = []
            flatten_configs = []
            for stack in stacks:
                for config in stack:
                    if config and config not in flatten_configs:
                        flatten_configs.append(config)
            for resource in backward_resources:
                clone = resource.clone(additional_metadata={"label": resource.label})
                for config in flatten_configs:
                    clone.url = append_filter_config(clone.url, config)
                backward_clones.append(clone)
            filter_id = ", ".join([f"{k}={v}" for config in flatten_configs for k, v in config.items() if k != "type"])
            yield forward_clones, backward_clones, filter_id

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
