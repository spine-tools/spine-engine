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
Contains the SpineEngine class for running Spine Toolbox DAGs.

:authors: M. Marin (KTH)
:date:   20.11.2019
"""

from enum import Enum, auto, unique
import os
import threading
import multiprocessing as mp
from itertools import product
import networkx as nx
from dagster import (
    PipelineDefinition,
    SolidDefinition,
    InputDefinition,
    OutputDefinition,
    DependencyDefinition,
    ModeDefinition,
    Output,
    Failure,
    DagsterEventType,
    default_executors,
    AssetMaterialization,
    reexecute_pipeline_iterator,
    DagsterInstance,
    execute_pipeline_iterator,
)
from spinedb_api import append_filter_config, name_from_dict
from spinedb_api.filters.tools import filter_config
from spinedb_api.filters.scenario_filter import scenario_name_from_dict
from spinedb_api.filters.execution_filter import execution_filter_config
from .exception import EngineInitFailed
from .execution_managers.persistent_execution_manager import (
    disable_persistent_process_creation,
    enable_persistent_process_creation,
)
from .utils.chunk import chunkify
from .utils.helpers import AppSettings, inverted, create_timestamp, make_dag
from .utils.execution_resources import one_shot_process_semaphore, persistent_process_semaphore
from .utils.queue_logger import QueueLogger
from .project_item_loader import ProjectItemLoader
from .multithread_executor.executor import multithread_executor
from .project_item.connection import Connection, Jump
from .shared_memory_io_manager import shared_memory_io_manager


@unique
class ExecutionDirection(Enum):
    FORWARD = auto()
    BACKWARD = auto()

    def __str__(self):
        return str(self.name)


ED = ExecutionDirection


@unique
class SpineEngineState(Enum):
    SLEEPING = 1
    """Dare to wake it?"""
    RUNNING = 2
    USER_STOPPED = 3
    FAILED = 4
    COMPLETED = 5

    def __str__(self):
        return str(self.name)


@unique
class ItemExecutionFinishState(Enum):
    SUCCESS = 1
    FAILURE = 2
    SKIPPED = 3
    EXCLUDED = 4
    STOPPED = 5
    NEVER_FINISHED = 6

    def __str__(self):
        return str(self.name)


class SpineEngine:
    """
    An engine for executing a Spine Toolbox DAG-workflow.
    """

    _resource_limit_lock = threading.Lock()

    def __init__(
        self,
        items=None,
        specifications=None,
        connections=None,
        jumps=None,
        items_module_name="spine_items",
        settings=None,
        project_dir=None,
        execution_permits=None,
        node_successors=None,
        debug=False,
    ):
        """
        Args:
            items (list(dict)): List of executable item dicts.
            specifications (dict(str,list(dict))): A mapping from item type to list of specification dicts.
            connections (list of dict): List of connection dicts
            jumps (list of dict, optional): List of jump dicts
            items_module_name (str): name of the Python module that contains project items
            settings (dict): Toolbox execution settings.
            project_dir (str): Path to project directory.
            execution_permits (dict(str,bool)): A mapping from item name to a boolean value, False indicating that
                the item is not executed, only its resources are collected.
            node_successors (dict(str,list(str))): A mapping from item name to list of successor item names,
                dictating the dependencies.
            debug (bool): Whether debug mode is active or not.

        Raises:
            EngineInitFailed: raised if initialization failed
        """
        super().__init__()
        self._queue = mp.Queue()
        if items is None:
            items = {}
        self._items = items
        if connections is None:
            connections = []
        self._connections = list(map(Connection.from_dict, connections))
        self._connections_by_source = dict()
        self._connections_by_destination = dict()
        for connection in self._connections:
            self._connections_by_source.setdefault(connection.source, list()).append(connection)
            self._connections_by_destination.setdefault(connection.destination, list()).append(connection)
        self._settings = AppSettings(settings if settings is not None else {})
        _set_resource_limits(self._settings, SpineEngine._resource_limit_lock)
        enable_persistent_process_creation()
        self._project_dir = project_dir
        project_item_loader = ProjectItemLoader()
        self._executable_item_classes = project_item_loader.load_executable_item_classes(items_module_name)
        if specifications is None:
            specifications = {}
        self._item_specifications = self._make_item_specifications(
            specifications, project_item_loader, items_module_name
        )
        self._solid_names = {item_name: str(i) for i, item_name in enumerate(items)}
        self._item_names = {solid_name: item_name for item_name, solid_name in self._solid_names.items()}
        if execution_permits is None:
            execution_permits = {}
        self._execution_permits = {self._solid_names[name]: permits for name, permits in execution_permits.items()}
        if node_successors is None:
            node_successors = {}
        dag = make_dag(node_successors)
        _validate_dag(dag)
        if jumps is None:
            jumps = []
        if not jumps:
            self._chunks = None
        else:
            jumps = list(map(Jump.from_dict, jumps))
            validate_jumps(jumps, dag)
            self._chunks = chunkify(dag, jumps)
        self._back_injectors = {
            self._solid_names[key]: [self._solid_names[x] for x in value] for key, value in node_successors.items()
        }
        self._forth_injectors = inverted(self._back_injectors)
        self._pipeline = self._make_pipeline()
        self._state = SpineEngineState.SLEEPING
        self._debug = debug
        self._running_items = []
        self._prompt_queues = {}
        self._event_stream = self._get_event_stream()
        self._backward_resources = {}
        self._forward_resources = {}
        self._timestamp = None

    def _make_item_specifications(self, specifications, project_item_loader, items_module_name):
        """Instantiates item specifications.

        Args:
            specifications (dict): A mapping from item type to list of specification dicts.
            project_item_loader (ProjectItemLoader): loader instance
            items_module_name (str): name of the Python module that contains the project items

        Returns:
            dict: mapping from item type to a dict that maps specification names to specification instances
        """
        specification_factories = project_item_loader.load_item_specification_factories(items_module_name)
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

    def _make_item(self, item_name, direction):
        """Recreates item from project item dictionary. Note that all items are created twice.
        One for the backward pipeline, the other one for the forward pipeline."""
        item_dict = self._items[item_name]
        item_type = item_dict["type"]
        executable_item_class = self._executable_item_classes[item_type]
        if direction == ED.FORWARD:
            prompt_queue = self._prompt_queues[item_name] = mp.Queue()
            logger = QueueLogger(self._queue, item_name, prompt_queue)
        else:
            logger = None  # Prevent backward solid from logging
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
        """Yields events (event_type, event_data).

        TODO: Describe the events in depth.

        Yields:
            tuple: event type and data
        """
        threading.Thread(target=self.run).start()
        while True:
            msg = self._queue.get()
            yield msg
            if msg[0] == "dag_exec_finished":
                break

    def answer_prompt(self, item_name, accepted):
        self._prompt_queues[item_name].put(accepted)

    def run(self):
        """Runs this engine."""
        self._timestamp = create_timestamp()
        if self._chunks is None:
            self._linear_run()
        else:
            self._chunked_run()

    def _linear_run(self):
        """Runs the engine without jumps and chunking."""
        self._state = SpineEngineState.RUNNING
        run_config = {"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}, "execution": {"multithread": {}}}
        for event in execute_pipeline_iterator(self._pipeline, run_config=run_config):
            self._process_event(event)
        if self._state == SpineEngineState.RUNNING:
            self._state = SpineEngineState.COMPLETED
        self._queue.put(("dag_exec_finished", str(self._state)))

    def _chunked_run(self):
        """Runs the engine with jumps and chunking."""
        self._state = SpineEngineState.RUNNING
        run_id = "root run"
        dagster_instance = DagsterInstance.ephemeral()
        try:
            dagster_instance.create_run_for_pipeline(self._pipeline, run_id=run_id)
            run_config = {
                "loggers": {"console": {"config": {"log_level": "CRITICAL"}}},
                "execution": {"multithread": {}},
            }
            self._run_chunks_backward(dagster_instance, run_id, run_config)
            self._run_chunks_forward(dagster_instance, run_config)
            if self._state == SpineEngineState.RUNNING:
                self._state = SpineEngineState.COMPLETED
            self._queue.put(("dag_exec_finished", str(self._state)))
        finally:
            dagster_instance.dispose()

    def _run_chunks_backward(self, dagster_instance, run_id, run_config):
        """Executes all backward solids.

        Args:
            dagster_instance (DagsterInstance): dagster instance
            run_id (str): root run id
            run_config (dict): dagster's run config
        """
        backward_solids = [f"{ED.BACKWARD}_{name}" for name in self._solid_names.values()]
        for event in reexecute_pipeline_iterator(
            self._pipeline, run_id, step_selection=backward_solids, run_config=run_config, instance=dagster_instance
        ):
            self._process_event(event)

    def _run_chunks_forward(self, dagster_instance, run_config):
        """Executes forward solids in chunks.

        Args:
            dagster_instance (DagsterInstance): dagster instance
            run_config (dict): dagster's run config
        """
        chunk_index = 0
        chunks_left = True
        loop_counters = dict()
        while chunks_left:
            chunk = self._chunks[chunk_index]
            run_id = next(iter(dagster_instance.get_runs())).run_id
            forward_solids = [f"{ED.FORWARD}_{self._solid_names[name]}" for name in chunk.item_names]
            for event in reexecute_pipeline_iterator(
                self._pipeline, run_id, step_selection=forward_solids, run_config=run_config, instance=dagster_instance
            ):
                self._process_event(event)
            if self._state in (SpineEngineState.FAILED, SpineEngineState.USER_STOPPED):
                break
            if chunk.jump is None:
                chunk_index += 1
            else:
                loop_counter = loop_counters.get(chunk_index, 0)
                loop_counter += 1
                loop_counters[chunk_index] = loop_counter
                chunk.jump.receive_resources_from_source(self._forward_resources.get(chunk.jump.source, []))
                chunk.jump.receive_resources_from_destination(self._backward_resources.get(chunk.jump.destination, []))
                logger = QueueLogger(self._queue, chunk.jump.name, None)
                if chunk.jump.is_condition_true(loop_counter, logger):
                    for i, other_chunk in enumerate(self._chunks[: chunk_index + 1]):
                        if chunk.jump.destination in other_chunk.item_names:
                            chunk_index = i
                            break
                else:
                    loop_counters[chunk_index] = 1
                    chunk_index += 1
            chunks_left = len(self._chunks) != chunk_index

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
                        "item_state": ItemExecutionFinishState.FAILURE,
                    },
                )
            )
            if self._debug:
                error = event.event_specific_data.error
                print("Traceback (most recent call last):")
                print("".join(error.stack + [error.message]))
                print("(reported by SpineEngine in debug mode)")
        elif event.event_type == DagsterEventType.STEP_SUCCESS:
            # Notify Toolbox here when BACKWARD execution has finished
            direction, _, solid_name = event.solid_name.partition("_")
            if direction != "BACKWARD":
                return
            item_name = self._item_names[solid_name]
            if not self._execution_permits[solid_name]:
                item_finish_state = ItemExecutionFinishState.EXCLUDED
            else:
                item_finish_state = ItemExecutionFinishState.SUCCESS
            self._queue.put(
                (
                    'exec_finished',
                    {
                        "item_name": item_name,
                        "direction": direction,
                        "state": str(self._state),
                        "item_state": item_finish_state,
                    },
                )
            )
        elif event.event_type == DagsterEventType.ASSET_MATERIALIZATION:
            # Notify Toolbox here when FORWARD execution has finished
            direction, _, solid_name = event.solid_name.partition("_")
            if direction != "FORWARD":
                return
            item_name = self._item_names[solid_name]
            state_value = event.asset_key.path[0]
            item_finish_state = ItemExecutionFinishState[state_value]
            self._queue.put(
                (
                    'exec_finished',
                    {
                        "item_name": item_name,
                        "direction": direction,
                        "state": str(self._state),
                        "item_state": item_finish_state,
                    },
                )
            )

    def stop(self):
        """Stops the engine."""
        self._state = SpineEngineState.USER_STOPPED
        disable_persistent_process_creation()
        for item in self._running_items:
            self._stop_item(item)
        self._queue.put(("dag_exec_finished", str(self._state)))

    def _stop_item(self, item):
        item.stop_execution()
        self._queue.put(
            (
                'exec_finished',
                {
                    "item_name": item.name,
                    "direction": str(ED.FORWARD),
                    "state": str(self._state),
                    "item_state": ItemExecutionFinishState.STOPPED,
                },
            )
        )

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
        mode_defs = [
            ModeDefinition(
                executor_defs=default_executors + [multithread_executor],
                resource_defs={"io_manager": shared_memory_io_manager},
            )
        ]
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
            item = self._make_item(item_name, ED.BACKWARD)
            self._backward_resources[item_name] = resources = item.output_resources(ED.BACKWARD)
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
            output_resource_stacks, item_finish_state = self._execute_item(
                context, item_name, forward_resource_stacks, backward_resources
            )
            self._forward_resources[item_name] = [r for stack in output_resource_stacks for r in stack]
            yield AssetMaterialization(asset_key=str(item_finish_state))
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
        success = [ItemExecutionFinishState.NEVER_FINISHED]
        output_resources_list = []
        threads = []
        resources_iterator = self._filtered_resources_iterator(
            item_name, forward_resource_stacks, backward_resources, self._timestamp
        )
        for flt_fwd_resources, flt_bwd_resources, filter_id in resources_iterator:
            item = self._make_item(item_name, ED.FORWARD)
            if not item.ready_to_execute(self._settings):
                if not self._execution_permits[self._solid_names[item_name]]:  # Exclude if not selected
                    success[0] = ItemExecutionFinishState.EXCLUDED
                else:  # Fail if selected
                    context.log.error(f"compute_fn() FAILURE in: '{item_name}', not ready for forward execution")
                    success[0] = ItemExecutionFinishState.FAILURE
            else:
                item.filter_id = filter_id
                thread = threading.Thread(
                    target=self._execute_item_filtered,
                    args=(item, flt_fwd_resources, flt_bwd_resources, output_resources_list, success),
                )
                threads.append(thread)
                thread.start()
        for thread in threads:
            thread.join()
        if success[0] == ItemExecutionFinishState.FAILURE:
            context.log.error(f"compute_fn() FAILURE with item: {item_name} failed to execute")
            raise Failure()
        for resources in output_resources_list:
            for connection in self._connections_by_source.get(item_name, []):
                connection.receive_resources_from_source(resources)
                if connection.has_filters():
                    connection.fetch_database_items()
        return output_resources_list, success[0]

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
            item_finish_state = item.execute(filtered_forward_resources, filtered_backward_resources)
            item.finish_execution(item_finish_state)
        else:
            item.exclude_execution(filtered_forward_resources, filtered_backward_resources)
            item_finish_state = ItemExecutionFinishState.EXCLUDED
        filter_stack = sum((r.metadata.get("filter_stack", ()) for r in filtered_forward_resources), ())
        output_resources = item.output_resources(ED.FORWARD)
        for resource in output_resources:
            resource.metadata["filter_stack"] = filter_stack
            resource.metadata["filter_id"] = item.filter_id
        output_resources_list.append(output_resources)
        success[0] = item_finish_state  # FIXME: We need a Lock here
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

        Yields:
            tuple(list,list,str): forward resources, backward resources, filter id
        """

        def check_resource_affinity(filtered_forward_resources):
            filter_ids_by_provider = dict()
            for r in filtered_forward_resources:
                filter_ids_by_provider.setdefault(r.provider_name, set()).add(r.metadata.get("filter_id"))
            return all(len(filter_ids) == 1 for filter_ids in filter_ids_by_provider.values())

        resource_filter_stacks = dict()
        non_filterable_resources = dict()
        filterable_resources = list()
        for stack in forward_resource_stacks:
            if not stack:
                continue
            non_filterable = list()
            for resource in stack:
                filter_stacks = self._filter_stacks(item_name, resource.provider_name, resource.label)
                if not filter_stacks:
                    non_filterable.append(resource)
                else:
                    resource_filter_stacks[(resource.provider_name, resource.label)] = filter_stacks
                    filterable_resources.append(resource)
            if non_filterable:
                non_filterable_resources.setdefault(stack[0].provider_name, list()).append(non_filterable)
        forward_resource_stacks_iterator = (
            self._expand_resource_stack(resource, resource_filter_stacks[(resource.provider_name, resource.label)])
            for resource in filterable_resources
        )
        for resources_or_lists in product(*non_filterable_resources.values(), *forward_resource_stacks_iterator):
            filtered_forward_resources = list()
            for item in resources_or_lists:
                if isinstance(item, list):
                    filtered_forward_resources += item
                else:
                    filtered_forward_resources.append(item)
            if not check_resource_affinity(filtered_forward_resources):
                continue
            filtered_forward_resources = self._convert_forward_resources(item_name, filtered_forward_resources)
            resource_filter_stack = {r: r.metadata.get("filter_stack", ()) for r in filtered_forward_resources}
            scenarios = {scenario_name_from_dict(cfg) for stack in resource_filter_stack.values() for cfg in stack}
            scenarios.discard(None)
            execution = {"execution_item": item_name, "scenarios": list(scenarios), "timestamp": timestamp}
            config = execution_filter_config(execution)
            filtered_backward_resources = []
            for resource in backward_resources:
                clone = resource.clone(additional_metadata={"filter_stack": (config,)})
                clone.url = append_filter_config(clone.url, config)
                filtered_backward_resources.append(clone)
            filter_id = _make_filter_id(resource_filter_stack)
            yield list(filtered_forward_resources), filtered_backward_resources, filter_id

    @staticmethod
    def _expand_resource_stack(resource, filter_stacks):
        """Expands a resource according to filters defined for that resource.

        Returns an expanded stack of as many resources as filter stacks defined for the resource.
        Each resource in the expanded stack is a clone of the original, with one of the filter stacks
        applied to the URL.

        Args:
            resource (ProjectItemResource): resource to expand
            filter_stacks (list): resource's filter stacks

        Returns:
            tuple(ProjectItemResource): expanded resources
        """
        expanded_stack = ()
        for filter_stack in filter_stacks:
            filtered_clone = resource.clone(additional_metadata={"filter_stack": filter_stack})
            for config in filter_stack:
                filtered_clone.url = append_filter_config(filtered_clone.url, config)
            expanded_stack += (filtered_clone,)
        return expanded_stack

    def _filter_stacks(self, item_name, provider_name, resource_label):
        """Computes filter stacks.

        Stacks are computed as the cross-product of all individual filters defined for a resource.

        Args:
            item_name (str): item's name
            provider_name (str): resource provider's name
            resource_label (str): resource's label

        Returns:
            list of list: filter stacks
        """
        connections = self._connections_by_destination.get(item_name, [])
        connection = None
        for c in connections:
            if c.source == provider_name:
                connection = c
                break
        if connection is None:
            raise RuntimeError("Logic error: no connection from resource provider")
        filters = connection.resource_filters.get(resource_label)
        if filters is None:
            return []
        filter_configs_list = []
        for filter_type, ids in filters.items():
            filter_configs = [
                filter_config(filter_type, connection.id_to_name(id_, filter_type))
                for id_, is_on in ids.items()
                if is_on
            ]
            if not filter_configs:
                continue
            filter_configs_list.append(filter_configs)
        return list(product(*filter_configs_list))

    def _convert_forward_resources(self, item_name, resources):
        """Converts resources as they're being forwarded to given item.
        The conversion is dictated by the connection the resources traverse in order to reach the item.

        Args:
            item_name (str): receiving item's name
            resources (Iterable of ProjectItemResource): resources to convert

        Returns:
            list of ProjectItemResource: converted resources
        """
        connections = self._connections_by_destination.get(item_name, [])
        resources_by_provider = {}
        for r in resources:
            resources_by_provider.setdefault(r.provider_name, list()).append(r)
        for c in connections:
            resources_from_source = resources_by_provider.get(c.source)
            if resources_from_source is None:
                continue
            resources_by_provider[c.source] = c.convert_resources(resources_from_source)
        return [r for resources in resources_by_provider.values() for r in resources]

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
    """Builds filter id from resource filter stack.

    Args:
        resource_filter_stack (dict): mapping from resource to filter stack

    Returns:
        str: filter id
    """
    provider_filters = set()
    for resource, stack in resource_filter_stack.items():
        if resource.type_ != "database":
            filter_id = resource.metadata.get("filter_id")
            if filter_id is None:
                continue
            provider_filters.add(filter_id)
        else:
            filter_names = sorted(_filter_names_from_stack(stack))
            if not filter_names:
                continue
            provider_filters.add(", ".join(filter_names) + " - " + resource.provider_name)
    return " & ".join(sorted(provider_filters))


def _filter_names_from_stack(stack):
    for config in stack:
        if not config:
            continue
        filter_name = name_from_dict(config)
        if filter_name is not None:
            yield filter_name


def _validate_dag(dag):
    """Raises an exception in case DAG is not valid.

    Args:
        dag (networkx.DiGraph): mapping from node name to list of direct successor nodes
    """
    if not nx.is_directed_acyclic_graph(dag):
        raise EngineInitFailed("Invalid DAG")
    if len(list(nx.weakly_connected_components(dag))) > 1:
        raise EngineInitFailed("DAG contains unconnected items.")


def validate_jumps(jumps, dag):
    """Raises an exception in case jumps are not valid.

    Args:
        jumps (list of Jump): jumps
        dag (DiGraph): jumps' DAG
    """
    jump_paths = dict()
    for jump in jumps:
        for other in jumps:
            if other is jump:
                continue
            if other.source == jump.source and other.destination == jump.destination:
                raise EngineInitFailed("Loops with same source and destination not supported.")
        if jump.source == jump.destination:
            continue
        if jump.source not in dag.nodes:
            raise EngineInitFailed(f"Loop source '{jump.source}' not found in DAG")
        if jump.source == jump.destination:
            continue
        if nx.has_path(dag, jump.source, jump.destination):
            raise EngineInitFailed("Cannot loop in forward direction.")
        if not nx.has_path(nx.reverse_view(dag), jump.source, jump.destination):
            raise EngineInitFailed("Cannot loop between DAG branches.")
        source_overlapping_jumps = set()
        destination_overlapping_jumps = set()
        for id_, path in jump_paths.items():
            if jump.source in path:
                source_overlapping_jumps.add(id_)
            if jump.destination in path:
                destination_overlapping_jumps.add(id_)
        if source_overlapping_jumps != destination_overlapping_jumps:
            raise EngineInitFailed("Partially overlapping loops not supported.")
        jump_paths[len(jump_paths)] = {
            item for path in nx.all_simple_paths(dag, jump.destination, jump.source) for item in path
        }


def _set_resource_limits(settings, lock):
    """Sets limits for simultaneous single-shot and persistent processes.

    May potentially kill existing persistent processes.

    Args:
        settings (AppSettings): Engine settings
    """
    with lock:
        single_shot_control = settings.value("engineSettings/processLimiter", "auto")
        if single_shot_control == "auto":
            limit = os.cpu_count()
        else:
            limit = int(settings.value("engineSettings/maxProcesses", os.cpu_count()))
        one_shot_process_semaphore.set_limit(limit)
        persistent_process_control = settings.value("engineSettings/persistentLimiter", "unlimited")
        if persistent_process_control == "unlimited":
            limit = "unlimited"
        elif persistent_process_control == "auto":
            limit = os.cpu_count()
        else:
            limit = int(settings.value("engineSettings/maxPersistentProcesses", os.cpu_count()))
        persistent_process_semaphore.set_limit(limit)
