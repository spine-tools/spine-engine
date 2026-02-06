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
"""Contains the SpineEngine class for running Spine Toolbox DAGs."""
from __future__ import annotations
from collections.abc import Iterator
from dataclasses import dataclass, field
from enum import Enum, unique
from itertools import product
import multiprocessing as mp
import os
import threading
from typing import TYPE_CHECKING, Literal, TypeAlias
import networkx as nx
from spinedb_api import append_filter_config, name_from_dict
from spinedb_api.filters.execution_filter import ExecutionDescriptor, execution_filter_config
from spinedb_api.filters.scenario_filter import scenario_name_from_dict
from spinedb_api.filters.tools import filter_config
from spinedb_api.spine_db_server import db_server_manager
from .exception import EngineInitFailed
from .execution_managers.persistent_execution_manager import (
    disable_persistent_process_creation,
    enable_persistent_process_creation,
)
from .jumpster import (
    Failure,
    Finalization,
    InputDefinition,
    JumpsterEvent,
    JumpsterEventType,
    Output,
    PipelineDefinition,
    SolidDefinition,
    execute_pipeline_iterator,
)
from .project_item.connection import Connection, Jump
from .project_item.executable_item_base import ExecutableItemBase
from .project_item.project_item_resource import ProjectItemResource
from .project_item.project_item_specification import ProjectItemSpecification
from .project_item_loader import ProjectItemLoader
from .utils.execution_resources import one_shot_process_semaphore, persistent_process_semaphore
from .utils.helpers import (
    AppSettings,
)
from .utils.helpers import (
    ItemExecutionFinishState,
    create_timestamp,
    dag_edges,
    inverted,
    make_connections,
    make_dag,
    required_items_for_execution,
)
from .utils.helpers import ExecutionDirection as ED
from .utils.queue_logger import QueueLogger

if TYPE_CHECKING:
    from multiprocessing.synchronize import Lock as LockType


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


class SuccessValue:
    def __init__(self):
        self.value = ItemExecutionFinishState.NEVER_FINISHED


EventType: TypeAlias = Literal[
    "dag_exec_finished",
    "event_msg",
    "exec_finished",
    "exec_started",
    "flash",
    "kernel_execution_msg",
    "persistent_execution_msg",
    "process_msg",
    "prompt",
    "server_status_msg",
    "standard_execution_msg",
]


class SpineEngine:
    """An engine for executing a Spine Toolbox DAG-workflow."""

    _resource_limit_lock = threading.Lock()

    def __init__(
        self,
        items: dict[str, dict] | None = None,
        specifications: dict[str, list[dict]] | None = None,
        connections: list[dict] | None = None,
        jumps: list[dict] | None = None,
        items_module_name: str = "spine_items",
        settings: dict | None = None,
        project_dir: str | None = None,
        execution_permits: dict[str, bool] | None = None,
        debug: bool = False,
    ):
        """
        Args:
            items: A mapping from item name to item dict
            specifications: A mapping from item type to list of specification dicts.
            connections: List of connection dicts
            jumps: List of jump dicts
            items_module_name: name of the Python module that contains project items
            settings: Toolbox execution settings.
            project_dir: Path to project directory.
            execution_permits : A mapping from item name to a boolean value, False indicating that
                the item is not executed
            debug: Whether debug mode is active or not.

        Raises:
            EngineInitFailed: Raised if initialization fails
        """
        self._queue = mp.Queue()
        if items is None:
            items = {}
        self._items = items
        if execution_permits is None:
            execution_permits = {}
        self._execution_permits = execution_permits
        connections = list(map(Connection.from_dict, connections))
        project_item_loader = ProjectItemLoader()
        self._executable_item_classes = project_item_loader.load_executable_item_classes(items_module_name)
        required_items = required_items_for_execution(
            self._items, connections, self._executable_item_classes, self._execution_permits
        )
        self._connections = make_connections(connections, required_items)
        self._connections_by_source: dict[str, list[Connection]] = {}
        self._connections_by_destination: dict[str, list[Connection]] = {}
        self._validate_and_sort_connections()
        self._back_injectors = dag_edges(
            self._connections
        )  # Mapping of a source node (item) to a list of destination nodes (items)
        self._check_write_index()
        self._settings = AppSettings(settings if settings is not None else {})
        _set_resource_limits(self._settings, SpineEngine._resource_limit_lock)
        enable_persistent_process_creation()
        self._project_dir = project_dir
        if specifications is None:
            specifications = {}
        self._item_specifications = self._make_item_specifications(
            specifications, project_item_loader, items_module_name
        )
        self._dag = make_dag(self._back_injectors, self._execution_permits)
        _validate_dag(self._dag)
        self._item_names: list[str] = list(self._dag)  # Names of permitted items and their neighbors
        if jumps is None:
            jumps = []
        else:
            jumps = list(map(Jump.from_dict, jumps))
        items_by_jump = _get_items_by_jump(jumps, self._dag)
        self._jumps = filter_unneeded_jumps(jumps, items_by_jump, execution_permits)
        validate_jumps(self._jumps, items_by_jump, self._dag)
        for x in self._connections + self._jumps:
            x.make_logger(self._queue)
        for x in self._jumps:
            x.set_engine(self)
        self._forth_injectors = inverted(self._back_injectors)
        self._pipeline = self._make_pipeline()
        self._state = SpineEngineState.SLEEPING
        self._debug = debug
        self._running_items = []
        self._prompt_queues = {}
        self._answered_prompts = {}
        self.resources_per_item = {}  # Tuples of (forward resources, backward resources) from last execution
        self._timestamp = create_timestamp()
        self._db_server_manager_queue = None
        self._thread = threading.Thread(target=self.run)
        self._event_stream = self._get_event_stream()

    def _descendants(self, name: str) -> Iterator[str]:
        """Yields descendant item names.

        Args:
            name: name of the project item whose descendants to collect

        Yields:
            descendant name
        """
        for c in self._connections_by_source.get(name, ()):
            yield c.destination
            yield from self._descendants(c.destination)

    def _check_write_index(self) -> None:
        """Checks if write indexes are valid."""
        conflicting_by_item = {}
        for item_name in self._items:
            conflicting = {}
            descendants = self._descendants(item_name)
            for conn in self._connections_by_source.get(item_name, ()):
                sibling_connections = [
                    x for x in self._connections_by_destination.get(conn.destination, []) if x != conn
                ]
                conflicting.update(
                    {
                        c.source: c.destination
                        for c in sibling_connections
                        if c.write_index < conn.write_index and c.source in descendants
                    }
                )
            if conflicting:
                conflicting_by_item[item_name] = conflicting
        rows = []
        for item_name, conflicting in conflicting_by_item.items():
            row = []
            for other_item_name, dest in conflicting.items():
                row.append(f"{other_item_name}, but {other_item_name} is set to write ealier to {dest}")
            if row:
                rows.append(f"Item {item_name} cannot execute because it is a dependency to " + ", ".join(row))
        msg = "\n".join(rows)
        if msg:
            raise EngineInitFailed(msg)

    def _validate_and_sort_connections(self) -> None:
        """Checks and sorts Connections by source and destination.

        Raises:
            EngineInitFailed: If connection is not ready
        """
        for connection in self._connections:
            if not connection.ready_to_execute():
                notifications = " ".join(connection.notifications())
                raise EngineInitFailed(f"Link {connection.name} is not ready for execution. {notifications}")
            source, destination = connection.source, connection.destination
            self._connections_by_source.setdefault(source, list()).append(connection)
            self._connections_by_destination.setdefault(destination, list()).append(connection)

    def _make_item_specifications(
        self, specifications: dict[str, list[dict]], project_item_loader: ProjectItemLoader, items_module_name: str
    ) -> dict[str, dict[str, ProjectItemSpecification]]:
        """Instantiates item specifications.

        Args:
            specifications: A mapping from item type to list of specification dicts.
            project_item_loader: loader instance
            items_module_name: name of the Python module that contains the project items

        Returns:
            Mapping from item type to a dict that maps specification names to specification instances.
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

    def make_item(self, item_name: str, direction: ED) -> ExecutableItemBase:
        """Recreates item from project item dictionary for a particular execution.
        Note that this method is called multiple times for each item:
        Once for the backward pipeline, and once for each filtered execution in the forward pipeline."""
        item_dict = self._items[item_name]
        prompt_queue = mp.Queue()
        self._prompt_queues[id(prompt_queue)] = prompt_queue
        logger = QueueLogger(
            self._queue, item_name, prompt_queue, self._answered_prompts, silent=direction is ED.BACKWARD
        )
        return self.do_make_item(item_name, item_dict, logger)

    def do_make_item(self, item_name: str, item_dict: dict, logger: QueueLogger) -> ExecutableItemBase:
        item_type = item_dict["type"]
        executable_item_class = self._executable_item_classes[item_type]
        return executable_item_class.from_dict(
            item_dict, item_name, self._project_dir, self._settings, self._item_specifications, logger
        )

    def get_event(self) -> tuple[EventType, dict]:
        """Returns the next event in the stream. Calling this after receiving the event of type "dag_exec_finished"
        will raise StopIterationError."""
        return next(self._event_stream)

    def state(self):
        """Returns Spine Engine state."""
        return self._state

    def _get_event_stream(self) -> Iterator[tuple[EventType, dict]]:
        """Yields events (event_type, event_data).

        TODO: Describe the events in depth.

        Yields:
            tuple: event type and data
        """
        self._thread.start()
        while True:
            msg = self._queue.get()
            if msg[0] == "dag_exec_finished":
                break
            yield msg
        yield msg
        self._thread.join()

    def answer_prompt(self, prompter_id: str, answer: str) -> None:
        """Answers the prompt for the specified prompter id."""
        self._prompt_queues[prompter_id].put(answer)

    def wait(self) -> None:
        """Waits until engine execution has finished."""
        if self._thread.is_alive():
            self._thread.join()

    def run(self) -> None:
        """Starts db server manager the engine."""
        with db_server_manager() as self._db_server_manager_queue:
            self._do_run()

    def _do_run(self) -> None:
        """Runs this engine."""
        self._state = SpineEngineState.RUNNING
        for event in execute_pipeline_iterator(self._pipeline):
            self._process_event(event)
        if self._state == SpineEngineState.RUNNING:
            self._state = SpineEngineState.COMPLETED
        self._queue.put(("dag_exec_finished", str(self._state)))

    def _process_event(self, event: JumpsterEvent) -> None:
        """Processes events from a pipeline."""
        if event.event_type == JumpsterEventType.STEP_START:
            self._queue.put(("exec_started", {"item_name": event.item_name, "direction": event.direction}))
        elif event.event_type == JumpsterEventType.STEP_FAILURE and self._state != SpineEngineState.USER_STOPPED:
            self._state = SpineEngineState.FAILED
            self._queue.put(
                (
                    "exec_finished",
                    {
                        "item_name": event.item_name,
                        "direction": event.direction,
                        "item_state": ItemExecutionFinishState.FAILURE,
                    },
                )
            )
            if self._debug:
                error = event.error
                print("Traceback (most recent call last):")
                print("".join(error.stack + [error.message]))
                print("(reported by SpineEngine in debug mode)")
        elif event.event_type == JumpsterEventType.STEP_FINISH:
            self._queue.put(
                (
                    "exec_finished",
                    {
                        "item_name": event.item_name,
                        "direction": event.direction,
                        "item_state": event.item_finish_state,
                    },
                )
            )

    def stop(self) -> None:
        """Stops the engine."""
        self._state = SpineEngineState.USER_STOPPED
        disable_persistent_process_creation()
        for item in self._running_items:
            self._stop_item(item)
        self._queue.put(("dag_exec_finished", str(self._state)))

    def _stop_item(self, item: ExecutableItemBase) -> None:
        """Stops given project item."""
        item.stop_execution()
        self._queue.put(
            (
                "exec_finished",
                {
                    "item_name": item.name,
                    "direction": ED.FORWARD,
                    "item_state": ItemExecutionFinishState.STOPPED,
                },
            )
        )

    def _make_pipeline(self) -> PipelineDefinition:
        """Returns a PipelineDefinition for executing this engine."""
        solid_defs = [
            make_solid_def(item_name)
            for item_name in self._item_names
            for make_solid_def in (self._make_forward_solid_def, self._make_backward_solid_def)
        ]
        self._complete_jumps()
        return PipelineDefinition(solid_defs=solid_defs, jumps=self._jumps)

    def _complete_jumps(self) -> None:
        """Updates jumps with item and corresponding solid information."""
        for jump in self._jumps:
            src, dst = jump.source, jump.destination
            jump.item_names = {dst, src}
            for path in nx.all_simple_paths(self._dag, dst, src):
                jump.item_names.update(path)

    def _make_backward_solid_def(self, item_name: str) -> SolidDefinition:
        """Returns a SolidDefinition for executing the given item in the backward sweep.

        Args:
            item_name: The project item that gets executed by the solid.

        Returns:
            solid's definition
        """

        def compute_fn(inputs):
            if self.state() == SpineEngineState.USER_STOPPED:
                raise Failure()
            item = self.make_item(item_name, ED.BACKWARD)
            resources = item.output_resources(ED.BACKWARD)
            for r in resources:
                r.metadata["db_server_manager_queue"] = self._db_server_manager_queue
            yield Output(value=resources)
            yield Finalization(
                item_finish_state=(
                    ItemExecutionFinishState.SUCCESS
                    if self._execution_permits[item_name]
                    else ItemExecutionFinishState.EXCLUDED
                )
            )

        return SolidDefinition(item_name=item_name, direction=ED.BACKWARD, input_defs=[], compute_fn=compute_fn)

    def _make_forward_solid_def(self, item_name: str) -> SolidDefinition:
        """Returns a SolidDefinition for executing the given item."""

        def compute_fn(inputs):
            if self.state() == SpineEngineState.USER_STOPPED:
                raise Failure()
            for conn in self._connections_by_destination.get(item_name, []):
                conn.visit_destination()
            # Split inputs into forward and backward resources based on prefix
            if ED.FORWARD in inputs:
                resource_pools = []
                for stack in inputs[ED.FORWARD]:
                    for resource in stack:
                        filter_stack = resource.metadata["filter_stack"]
                        for pool in resource_pools:
                            if filter_stack == pool.filter_stack:
                                pool.resources.append(resource)
                                break
                        else:
                            resource_pools.append(_ResourcePool([resource], filter_stack))
                if len(resource_pools) > 1:
                    resource_pools = _merge_pools(resource_pools)
                    resource_pools = _distribute_stackless_resources_to_all_pools(resource_pools)
                forward_resource_stacks = [pool.resources for pool in resource_pools]
            else:
                forward_resource_stacks = []
            if ED.BACKWARD in inputs:
                backward_resources = inputs[ED.BACKWARD]
            else:
                backward_resources = []
            item_finish_state, output_resource_stacks = self._execute_item(
                item_name, forward_resource_stacks, backward_resources
            )
            if output_resource_stacks:
                yield Output(value=output_resource_stacks)
            for conn in self._connections_by_source.get(item_name, []):
                conn.visit_source()
            yield Finalization(item_finish_state=item_finish_state)

        input_defs = [
            InputDefinition(item_name=inj, direction=ED.FORWARD) for inj in self._forth_injectors.get(item_name, [])
        ] + [InputDefinition(item_name=inj, direction=ED.BACKWARD) for inj in self._back_injectors.get(item_name, [])]
        return SolidDefinition(item_name=item_name, direction=ED.FORWARD, input_defs=input_defs, compute_fn=compute_fn)

    def _execute_item(
        self,
        item_name: str,
        forward_resource_stacks: list[tuple[ProjectItemResource, ...]],
        backward_resources: list[ProjectItemResource],
    ) -> tuple[ItemExecutionFinishState, list[list[ProjectItemResource]]]:
        """Executes the given item using the given forward resource stacks and backward resources.
        Returns list of output resource stacks.

        Called by ``_make_forward_solid_def.compute_fn``.

        For each element yielded by ``_filtered_resources_iterator``, spawns a thread that runs
        ``_execute_item_filtered``.

        Args:
            item_name: Item's name.
            forward_resource_stacks: resources coming from predecessor items -
                one tuple of ProjectItemResource per item, where each element in the tuple corresponds to a filtered
                execution of the item.
            backward_resources : resources coming from successor items - just one resource per item.

        Returns:
            Execution finish state, output resources.
        """
        item = self.make_item(item_name, ED.NONE)
        if not item.ready_to_execute(self._settings):
            if not self._execution_permits[item_name]:
                return ItemExecutionFinishState.EXCLUDED, []
            return ItemExecutionFinishState.FAILURE, []
        success = SuccessValue()
        output_resources_list = []
        threads = []
        resources_iterator = self._filtered_resources_iterator(
            item_name, forward_resource_stacks, backward_resources, self._timestamp
        )
        with mp.Manager() as multiprocess_manager:
            item_lock = multiprocess_manager.Lock()
            for flt_fwd_resources, flt_bwd_resources, filter_id in resources_iterator:
                self.resources_per_item[item_name] = (flt_fwd_resources, flt_bwd_resources)
                item = self.make_item(item_name, ED.FORWARD)
                item.filter_id = filter_id
                thread = threading.Thread(
                    target=self._execute_item_filtered,
                    args=(item, flt_fwd_resources, flt_bwd_resources, output_resources_list, item_lock, success),
                )
                threads.append(thread)
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        if success.value == ItemExecutionFinishState.FAILURE:
            raise Failure()
        for resources in output_resources_list:
            for connection in self._connections_by_source.get(item_name, []):
                connection.receive_resources_from_source(resources)
        return success.value, output_resources_list

    def _execute_item_filtered(
        self,
        item: ExecutableItemBase,
        filtered_forward_resources: list[ProjectItemResource],
        filtered_backward_resources: list[ProjectItemResource],
        output_resources_list: list[list[ProjectItemResource]],
        item_lock: LockType,
        success: SuccessValue,
    ) -> None:
        """Executes the given item using the given filtered resources. Target for threads in ``_execute_item``.

        Args:
            item: Executable item instance.
            filtered_forward_resources: Item's forward resources.
            filtered_backward_resources: Items's backward resources.
            output_resources_list: A list to append the output resources
                generated by the item.
            item_lock: Shared lock for parallel executions.
            success: The outcome of the execution.
        """
        self._running_items.append(item)
        if self._execution_permits[item.name]:
            item_finish_state = item.execute(filtered_forward_resources, filtered_backward_resources, item_lock)
            item.finish_execution(item_finish_state)
        else:
            item.exclude_execution(filtered_forward_resources, filtered_backward_resources, item_lock)
            item_finish_state = ItemExecutionFinishState.EXCLUDED
        filter_stack = []
        for fw_resource in filtered_forward_resources:
            if "filter_stack" not in fw_resource.metadata:
                continue
            resource_filter_stack = fw_resource.metadata["filter_stack"]
            if resource_filter_stack not in filter_stack:
                filter_stack.append(resource_filter_stack)
        filter_stack = sum(filter_stack, ())
        output_resources = item.output_resources(ED.FORWARD)
        for resource in output_resources:
            resource.metadata["filter_stack"] = filter_stack
            resource.metadata["filter_id"] = item.filter_id
            resource.metadata["db_server_manager_queue"] = self._db_server_manager_queue
        output_resources_list.append(output_resources)
        success.value = item_finish_state  # FIXME: We need a Lock here
        self._running_items.remove(item)

    def _filtered_resources_iterator(
        self,
        item_name: str,
        forward_resource_stacks: list[tuple[ProjectItemResource, ...]],
        backward_resources: list[ProjectItemResource],
        timestamp: str,
    ) -> Iterator[tuple[list[ProjectItemResource], list[ProjectItemResource], str]]:
        """Yields tuples of (filtered forward resources, filtered backward resources, filter id).

        Each tuple corresponds to a unique filter combination. Combinations are obtained by applying the cross-product
        over forward resource stacks.

        Args:
            item_name: item's name
            forward_resource_stacks: resources coming from predecessor items -
                one tuple of ProjectItemResource per item, where each element in the tuple corresponds to a filtered
                execution of the item.
            backward_resources: resources coming from successor items - just one resource per item.
            timestamp: timestamp for the execution filter

        Yields:
            forward resources, backward resources, filter id
        """

        def check_resource_affinity(filtered_forward_resources):
            filter_ids_by_provider = dict()
            for r in filtered_forward_resources:
                filter_ids_by_provider.setdefault(r.provider_name, set()).add(r.metadata.get("filter_id"))
            return all(len(filter_ids) == 1 for filter_ids in filter_ids_by_provider.values())

        resource_filter_stacks = dict()
        unfiltered_resource_lists = dict()
        for stack in forward_resource_stacks:
            if not stack:
                continue
            unfiltered = list()
            for resource in stack:
                filter_stacks = self._filter_stacks(item_name, resource.provider_name, resource.label)
                if not filter_stacks:
                    unfiltered.append(resource)
                else:
                    resource_filter_stacks[resource] = filter_stacks
            if unfiltered:
                unfiltered_resource_lists.setdefault(stack[0].provider_name, list()).append(unfiltered)
        forward_resource_stacks_iterator = (
            self._expand_resource_stack(resource, filter_stacks)
            for resource, filter_stacks in resource_filter_stacks.items()
        )
        backward_resources = self._convert_backward_resources(item_name, backward_resources)
        for resources_or_lists in product(*unfiltered_resource_lists.values(), *forward_resource_stacks_iterator):
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
            execution: ExecutionDescriptor = {
                "execution_item": item_name,
                "scenarios": list(scenarios),
                "timestamp": timestamp,
            }
            config = execution_filter_config(execution)
            filtered_backward_resources = []
            for resource in backward_resources:
                if "part_count" in resource.metadata:
                    resource.metadata["part_count"] += 1
                clone = resource.clone(additional_metadata={"filter_stack": (config,)})
                clone.url = append_filter_config(clone.url, config)
                filtered_backward_resources.append(clone)
            filter_id = _make_filter_id(resource_filter_stack)
            yield list(filtered_forward_resources), filtered_backward_resources, filter_id

    @staticmethod
    def _expand_resource_stack(
        resource: ProjectItemResource, filter_stacks: list[tuple[dict, ...]]
    ) -> tuple[ProjectItemResource, ...]:
        """Expands a resource according to filters defined for that resource.

        Returns an expanded stack of as many resources as filter stacks defined for the resource.
        Each resource in the expanded stack is a clone of the original, with one of the filter stacks
        applied to the URL.

        Args:
            resource: resource to expand
            filter_stacks: resource's filter stacks

        Returns:
            expanded resources
        """
        expanded_stack = ()
        for filter_stack in filter_stacks:
            filtered_clone = resource.clone(additional_metadata={"filter_stack": filter_stack})
            for config in filter_stack:
                filtered_clone.url = append_filter_config(filtered_clone.url, config)
            expanded_stack += (filtered_clone,)
        return expanded_stack

    def _filter_stacks(self, item_name: str, provider_name: str, resource_label: str) -> list[tuple[dict, ...]]:
        """Computes filter stacks.

        Stacks are computed as the cross-product of all individual filters defined for a resource.

        Args:
            item_name: item's name
            provider_name: resource provider's name
            resource_label: resource's label

        Returns:
            filter stacks
        """
        connections = self._connections_by_destination.get(item_name, [])
        connection = next(iter(c for c in connections if c.source == provider_name), None)
        if connection is None:
            raise RuntimeError("Logic error: no connection from resource provider")
        filters = connection.enabled_filters(resource_label)
        if filters is None:
            return []
        filter_configs_list = []
        for filter_type, values in filters.items():
            filter_configs = [filter_config(filter_type, value) for value in values]
            if not filter_configs:
                continue
            filter_configs_list.append(filter_configs)
        return list(product(*filter_configs_list))

    def _convert_backward_resources(
        self, item_name: str, resources: list[ProjectItemResource]
    ) -> list[ProjectItemResource]:
        """Converts resources as they're being passed backwards to given item.
        The conversion is dictated by the connection the resources traverse in order to reach the item.

        Args:
            item_name: receiving item's name
            resources: resources to convert

        Returns:
            converted resources
        """
        connections = self._connections_by_source.get(item_name, [])
        resources_by_provider = {}
        for r in resources:
            resources_by_provider.setdefault(r.provider_name, []).append(r)
        for c in connections:
            resources_from_destination = resources_by_provider.get(c.destination)
            if resources_from_destination is None:
                continue
            if self._execution_permits[item_name]:
                c.clean_up_backward_resources(resources_from_destination)
            sibling_connections = [x for x in self._connections_by_destination.get(c.destination, []) if x != c]
            resources_by_provider[c.destination] = c.convert_backward_resources(
                resources_from_destination, sibling_connections
            )
        return [r for resources in resources_by_provider.values() for r in resources]

    def _convert_forward_resources(
        self, item_name: str, resources: list[ProjectItemResource]
    ) -> list[ProjectItemResource]:
        """Converts resources as they're being passed forwards to given item.
        The conversion is dictated by the connection the resources traverse in order to reach the item.

        Args:
            item_name: receiving item's name
            resources: resources to convert

        Returns:
            converted resources
        """
        connections = self._connections_by_destination.get(item_name, [])
        resources_by_provider = {}
        for r in resources:
            resources_by_provider.setdefault(r.provider_name, list()).append(r)
        for c in connections:
            resources_from_source = resources_by_provider.get(c.source)
            if resources_from_source is None:
                continue
            resources_by_provider[c.source] = c.convert_forward_resources(resources_from_source)
        return [r for resources in resources_by_provider.values() for r in resources]


@dataclass
class _ResourcePool:
    resources: list[ProjectItemResource] = field(default_factory=list)
    filter_stack: tuple[dict] = field(default_factory=tuple)


def _merge_pools(resource_pools: list[_ResourcePool]) -> list[_ResourcePool]:
    merged_pool_i = _find_and_merge_pools(resource_pools)
    while merged_pool_i is not None:
        resource_pools = resource_pools[:merged_pool_i] + resource_pools[merged_pool_i + 1 :]
        merged_pool_i = _find_and_merge_pools(resource_pools)
    return resource_pools


def _find_and_merge_pools(resource_pools: list[_ResourcePool]) -> int | None:
    for i, pool in enumerate(resource_pools):
        if not pool.filter_stack:
            continue
        for target_pool in resource_pools[:i] + resource_pools[i + 1 :]:
            if all(filter_cfg in target_pool.filter_stack for filter_cfg in pool.filter_stack):
                target_pool.resources.extend(pool.resources)
                return i
    return None


def _distribute_stackless_resources_to_all_pools(resource_pools: list[_ResourcePool]) -> list[_ResourcePool]:
    distributed_pools = []
    for i, pool in enumerate(resource_pools):
        if pool.filter_stack == ():
            for target_pool in resource_pools[:i] + resource_pools[i + 1 :]:
                target_pool.resources.extend(pool.resources)
                distributed_pools.append(target_pool)
            break
    else:
        return resource_pools
    return distributed_pools


def _make_filter_id(resource_filter_stack: dict[ProjectItemResource, tuple[dict, ...]]) -> str:
    """Builds filter id from resource filter stack.

    Args:
        resource_filter_stack: mapping from resource to filter stack

    Returns:
        filter id
    """
    provider_filters = set()
    for resource, stack in resource_filter_stack.items():
        if resource.type_ != "database":
            filter_id = resource.metadata.get("filter_id")
            if not filter_id:
                continue
            provider_filters.add(filter_id)
        else:
            filter_names = sorted(_filter_names_from_stack(stack))
            if not filter_names:
                continue
            provider_filters.add(", ".join(filter_names) + " - " + resource.provider_name)
    return " & ".join(sorted(provider_filters))


def _filter_names_from_stack(stack: tuple[dict, ...]) -> Iterator[str]:
    """Yields filter names from filter stack.

    Args:
        stack: filter stack

    Yields:
        filter name
    """
    for config in stack:
        if not config:
            continue
        filter_name = name_from_dict(config)
        if filter_name is not None:
            yield filter_name


def _validate_dag(dag: nx.DiGraph) -> None:
    """Raises an exception in case DAG is not valid.

    Args:
        dag: mapping from node name to list of direct successor nodes
    """
    if not nx.is_directed_acyclic_graph(dag):
        raise EngineInitFailed("Invalid DAG")
    if len(list(nx.weakly_connected_components(dag))) > 1:
        raise EngineInitFailed("DAG contains unconnected items.")


def filter_unneeded_jumps(jumps, items_by_jump, execution_permits):
    """Drops jumps whose items are not going to be executed.

    Args:
        jumps (Iterable of Jump): jumps to filter
        items_by_jump (dict): mapping from jump to list of item names
        execution_permits (dict): mapping from item name to boolean telling if its is permitted to execute
    """
    return [jump for jump in jumps if all(execution_permits[item] for item in items_by_jump[jump])]


def validate_jumps(jumps, items_by_jump, dag):
    """Raises an exception in case jumps are not valid.

    Args:
        jumps (list of Jump): jumps
        items_by_jump (dict): mapping from jump to list of item names
        dag (DiGraph): jumps' DAG
    """
    for jump in jumps:
        validate_single_jump(jump, jumps, dag, items_by_jump)


def validate_single_jump(jump, jumps, dag, items_by_jump=None):
    """Raises an exception in case one jump is not valid.

    Args:
        jump (Jump): the jump to check
        jumps (list of Jump): all jumps in DAG
        dag (DiGraph): jumps' DAG
        items_by_jump (dict, optional): mapping jumps to a set of items in between destination and source
    """
    if not jump.ready_to_execute():
        raise EngineInitFailed(f"Jump {jump.name} is not ready for execution.")
    if items_by_jump is None:
        items_by_jump = _get_items_by_jump(jumps, dag)
    for other in jumps:
        if other is jump:
            continue
        if other.source == jump.source:
            raise EngineInitFailed(f"{jump.name} cannot have the same source as {other.name}.")
        jump_items = items_by_jump[jump]
        other_items = items_by_jump[other]
        intersection = jump_items & other_items
        if intersection not in (set(), jump_items, other_items):
            raise EngineInitFailed(f"{jump.name} cannot partially overlap {other.name}.")
    if not dag.has_node(jump.destination):
        raise EngineInitFailed(f"Loop destination '{jump.destination}' not found in DAG")
    if not dag.has_node(jump.source):
        raise EngineInitFailed(f"Loop source '{jump.source}' not found in DAG")
    if jump.source == jump.destination:
        return
    if nx.has_path(dag, jump.source, jump.destination):
        raise EngineInitFailed("Cannot loop in forward direction.")
    if not nx.has_path(nx.reverse_view(dag), jump.source, jump.destination):
        raise EngineInitFailed("Cannot loop between DAG branches.")


def _get_items_by_jump(jumps: list[Jump], dag: nx.DiGraph) -> dict[Jump, set]:
    """Returns a dict mapping jumps to a set of items between destination and source.

    Args:
        jumps: all jumps in dag
        dag: jumps' DAG

    Returns:
        Mapping from jump to list of items between destination and source.
    """
    items_by_jump = {}
    for jump in jumps:
        try:
            items_by_jump[jump] = {
                item for path in nx.all_simple_paths(dag, jump.destination, jump.source) for item in path
            }
        except nx.NodeNotFound:
            items_by_jump[jump] = set()
    return items_by_jump


def _set_resource_limits(settings: AppSettings, lock: LockType) -> None:
    """Sets limits for simultaneous single-shot and persistent processes.

    May potentially kill existing persistent processes.

    Args:
        settings: Engine settings
        lock: Multiprocessing lock.
    """
    with lock:
        process_limiter = settings.value("engineSettings/processLimiter", "auto")
        if process_limiter == "unlimited":
            limit = "unlimited"
        elif process_limiter == "auto":
            limit = os.cpu_count()
        else:
            limit = int(settings.value("engineSettings/maxProcesses", os.cpu_count()))
        one_shot_process_semaphore.set_limit(limit)
        persistent_limiter = settings.value("engineSettings/persistentLimiter", "unlimited")
        if persistent_limiter == "unlimited":
            limit = "unlimited"
        elif persistent_limiter == "auto":
            limit = os.cpu_count()
        else:
            limit = int(settings.value("engineSettings/maxPersistentProcesses", os.cpu_count()))
        persistent_process_semaphore.set_limit(limit)
