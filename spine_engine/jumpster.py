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

"""
In-house dagster replacement that also handles jumps.

"""

import sys
import threading
import queue
from enum import Enum, unique
from collections import namedtuple
from .utils.helpers import serializable_error_info_from_exc_info, ExecutionDirection as ED


@unique
class JumpsterEventType(Enum):
    STEP_START = 1
    STEP_FAILURE = 2
    STEP_SUCCESS = 3
    ASSET_MATERIALIZATION = 4


class PipelineDefinition:
    def __init__(self, solid_defs, jumps):
        self.solid_defs = solid_defs
        self.jumps = jumps


class DefinitionBase:
    def __init__(self, item_name, direction):
        self.item_name = item_name
        self.direction = direction

    @property
    def key(self):
        return (self.item_name, self.direction)


class SolidDefinition(DefinitionBase):
    def __init__(self, item_name, direction, input_defs, compute_fn):
        super().__init__(item_name, direction)
        self.input_defs = input_defs
        self.compute_fn = compute_fn


class InputDefinition(DefinitionBase):
    pass


class JumpsterEvent(DefinitionBase):
    def __init__(self, event_type, item_name, direction, **kwargs):
        super().__init__(item_name, direction)
        self.event_type = event_type
        for (key, value) in kwargs.items():
            setattr(self, key, value)


class Failure(BaseException):
    """A failure"""


class Output:
    def __init__(self, value):
        self.value = value


class AssetMaterialization:
    def __init__(self, asset_key):
        self.asset_key = asset_key


class JumpsterThreadError(Exception):
    """An exception has occurred in one or more of the threads dagster manages.
    This error forwards the message and stack trace for all of the collected errors.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.thread_error_infos = kwargs.pop("thread_error_infos")


class ActiveExecution:
    def __init__(self, pipeline_def):
        self._pipeline_def = pipeline_def
        self._output_value = {}
        self._ready_to_execute = {}
        self._in_flight = set()
        self._completed = set()
        self._succeeded = set()
        self._counters = {}
        for solid_def in self._pipeline_def.solid_defs:
            if not solid_def.input_defs:
                self._add_step(solid_def)
            else:
                counter = {"current": 0, "target": len(solid_def.input_defs), "solid_def": solid_def}
                for input_def in solid_def.input_defs:
                    self._counters.setdefault(input_def.key, {})[solid_def.key] = counter

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def is_complete(self):
        self._update()
        return not self._in_flight and not self._ready_to_execute

    def get_steps_to_execute(self, limit):
        steps = []
        while self._ready_to_execute and len(steps) < limit:
            key, step = self._ready_to_execute.popitem()
            self._in_flight.add(key)
            steps.append(step)
        return steps

    def _update(self):
        while self._succeeded:
            key = self._succeeded.pop()
            counters = self._counters.get(key, {})
            to_remove = set()
            for k, counter in counters.items():
                counter["current"] += 1
                if counter["current"] == counter["target"]:
                    self._add_step(counter["solid_def"])
                    to_remove.add(k)
            for k in to_remove:
                del counters[k]

    def _add_step(self, solid_def):
        step_inputs = {}
        for input_def in solid_def.input_defs:
            step_inputs.setdefault(input_def.direction, []).extend(self._output_value[input_def.key])
        step = Step(self, solid_def, step_inputs)
        self._ready_to_execute[step.key] = step

    def handle_event(self, event):
        if event.event_type in (JumpsterEventType.STEP_SUCCESS, JumpsterEventType.STEP_FAILURE):
            self._completed.add(event.key)
            self._in_flight.discard(event.key)
            if event.event_type == JumpsterEventType.STEP_SUCCESS:
                self._succeeded.add(event.key)


class Step:
    def __init__(self, active_execution, solid_def, step_inputs):
        self._active_execution = active_execution
        self._solid_def = solid_def
        self._step_inputs = step_inputs

    @property
    def item_name(self):
        return self._solid_def.item_name

    @property
    def direction(self):
        return self._solid_def.direction

    @property
    def key(self):
        return self._solid_def.key

    def execute(self):
        yield JumpsterEvent(JumpsterEventType.STEP_START, *self.key)
        try:
            for x in self._solid_def.compute_fn(self._step_inputs):
                if isinstance(x, Output):
                    self._active_execution._output_value[self.key] = x.value
                elif isinstance(x, AssetMaterialization):
                    yield JumpsterEvent(JumpsterEventType.ASSET_MATERIALIZATION, *self.key, asset_key=x.asset_key)
        except Exception as err:
            yield JumpsterEvent(JumpsterEventType.STEP_FAILURE, *self.key, error=err)
        else:
            yield JumpsterEvent(JumpsterEventType.STEP_SUCCESS, *self.key)

    def get_execution_dependency_keys(self):
        return set(input_def.key for input_def in self._solid_def.input_defs)


class MultithreadExecutor:
    def __init__(self, max_concurrent=None):
        self.max_concurrent = max_concurrent if max_concurrent else 100  # TODO: How to determine a good amount?

    def execute(self, pipeline_def):

        limit = self.max_concurrent

        with ActiveExecution(pipeline_def) as active_execution:
            active_iters = {}
            errors = {}
            waiting = {}
            iterating = {}
            iterating_active = set()
            iterating_failed = set()
            jumps = pipeline_def.jumps
            jump_by_source = {}
            jump_by_item_name = {}
            for jump in jumps:
                jump_by_source[jump.source] = jump
                non_nested_item_names = set(jump.item_names)
                for other_jump in jumps:
                    if jump is other_jump:
                        continue
                    if other_jump.item_names > jump.item_names:
                        continue
                    non_nested_item_names -= other_jump.item_names
                for item_name in non_nested_item_names:
                    jump_by_item_name[item_name] = jump
            unfinished_jumps = set(jumps)
            loop_iteration_counters = {}
            steps_by_key = {}
            while not active_execution.is_complete or active_iters:
                # start iterators
                while len(active_iters) < limit:
                    candidate_steps = active_execution.get_steps_to_execute(limit=(limit - len(active_iters)))
                    steps_by_key.update({step.key: step for step in candidate_steps})
                    # Add all waiting steps
                    candidate_steps += list(waiting.values())
                    # Add iterating steps that don't depend on other pending iterating
                    iterating_skipped = set()
                    for key, step in iterating.items():
                        dependency_keys = step.get_execution_dependency_keys()
                        if dependency_keys & (iterating_active | iterating_skipped | iterating_failed):
                            iterating_skipped.add(key)
                            continue
                        iterating_active.add(key)
                        candidate_steps.append(step)

                    executable_steps = []
                    for step in candidate_steps:
                        if step.direction == ED.BACKWARD:
                            executable_steps.append(step)
                            continue
                        # Check if the step depends on any jumps that don't contain it
                        predecessor_jumps = (jump for jump in unfinished_jumps if step.item_name not in jump.item_names)
                        predecessor_item_names = {item for jump in predecessor_jumps for item in jump.item_names}
                        predecessor_keys = {
                            k
                            for k, s in steps_by_key.items()
                            if s.direction == ED.FORWARD and s.item_name in predecessor_item_names
                        }
                        dependency_keys = step.get_execution_dependency_keys()
                        if dependency_keys & predecessor_keys:
                            if step.key not in iterating:
                                waiting[step.key] = step
                            continue
                        waiting.pop(step.key, None)
                        iterating.pop(step.key, None)
                        executable_steps.append(step)

                    if not executable_steps:
                        break

                    for step in executable_steps:
                        active_iters[step.key] = execute_step_in_thread(step, errors)
                # process active iterators
                empty_iters = []
                for key, step_iter in active_iters.items():
                    try:
                        event_or_none = next(step_iter)
                        if event_or_none is None:
                            continue
                        yield event_or_none
                        active_execution.handle_event(event_or_none)
                        step = steps_by_key[key]
                        if step.direction == ED.BACKWARD:
                            continue
                        # Handle loops
                        iterating_active.discard(key)
                        if event_or_none.event_type == JumpsterEventType.STEP_FAILURE:
                            # Mark failed loops as finished
                            failed_jump = jump_by_item_name.get(step.item_name)
                            if failed_jump is None:
                                continue
                            failed_item_names = failed_jump.item_names
                            for jump in jumps:
                                if jump.item_names & failed_item_names:
                                    unfinished_jumps.discard(jump)
                                    loop_iteration_counters.pop(jump, None)
                            iterating_failed.add(step)
                        elif event_or_none.event_type == JumpsterEventType.STEP_SUCCESS:
                            # Process loop condition
                            jump = jump_by_source.get(step.item_name)
                            if jump is None:
                                continue
                            forward_resources = [
                                r
                                for stack in active_execution._output_value.get((jump.source, ED.FORWARD), [])
                                for r in stack
                            ]
                            backward_resources = active_execution._output_value.get((jump.destination, ED.BACKWARD), [])
                            jump.receive_resources_from_source(forward_resources)
                            jump.receive_resources_from_destination(backward_resources)
                            iteration_counter = loop_iteration_counters.setdefault(jump, 1)
                            if jump.is_condition_true(iteration_counter):
                                # Put all jump steps in the iterating bucket
                                for k, s in steps_by_key.items():
                                    if s.direction == ED.FORWARD and s.item_name in jump.item_names:
                                        iterating[k] = s
                                # Mark all nested jumps unfinished again
                                for item_name in jump.item_names:
                                    nested_jump = jump_by_item_name.get(item_name)
                                    if nested_jump is not None:
                                        unfinished_jumps.add(nested_jump)

                                loop_iteration_counters[jump] += 1
                            else:
                                unfinished_jumps.remove(jump)
                                del loop_iteration_counters[jump]

                    except ThreadCrashException:
                        serializable_error = serializable_error_info_from_exc_info(sys.exc_info())
                        step_failure_event = JumpsterEvent(
                            JumpsterEventType.STEP_FAILURE, *key, error=serializable_error
                        )
                        active_execution.handle_event(step_failure_event)
                        yield step_failure_event
                        empty_iters.append(key)
                    except StopIteration:
                        empty_iters.append(key)
                        # TODO: Anything about loops?
                # clear and mark complete finished iterators
                for key in empty_iters:
                    del active_iters[key]

            errs = {tid: err for tid, err in errors.items() if err}
            if errs:
                raise JumpsterThreadError(
                    "During multithread execution errors occurred in threads:\n{error_list}".format(
                        error_list="\n".join(
                            ["In thread {tid}: {err}".format(tid=tid, err=err.to_string()) for tid, err in errs.items()]
                        )
                    ),
                    thread_error_infos=list(errs.values()),
                )


def execute_step_in_thread(step, errors):
    for ret in execute_thread_step(step):
        if ret is None or isinstance(ret, JumpsterEvent):
            yield ret
        elif isinstance(ret, ThreadEvent):
            if isinstance(ret, ThreadSystemErrorEvent):
                errors[ret.tid] = ret.error_info


# Thread execution stuff


class ThreadEvent:
    pass


class ThreadStartEvent(namedtuple("ThreadStartEvent", "tid"), ThreadEvent):
    pass


class ThreadDoneEvent(namedtuple("ThreadDoneEvent", "tid"), ThreadEvent):
    pass


class ThreadSystemErrorEvent(namedtuple("ThreadSystemErrorEvent", "tid error_info"), ThreadEvent):
    pass


class ThreadCrashException(Exception):
    """Thrown when the thread crashes."""


def _execute_step_in_thread(event_queue, step):
    """Wraps the execution of a command.

    Handles errors and communicates across a queue with the parent thread.

    Args:
        event_queue (Queue): event queue
        step (Step): execution step
    """
    tid = threading.get_ident()
    event_queue.put(ThreadStartEvent(tid=tid))
    try:
        for step_event in step.execute():
            event_queue.put(step_event)
        event_queue.put(ThreadDoneEvent(tid=tid))
    except Exception:  # pylint: disable=broad-except
        event_queue.put(
            ThreadSystemErrorEvent(tid=tid, error_info=serializable_error_info_from_exc_info(sys.exc_info()))
        )


TICK = 20.0 * 1.0 / 1000.0
"""The minimum interval at which to check for child process liveness -- default 20ms."""

THREAD_DEAD_AND_QUEUE_EMPTY = "THREAD_DEAD_AND_QUEUE_EMPTY"
"""Sentinel value."""


def _poll_for_event(thread, event_queue):
    try:
        return event_queue.get(block=True, timeout=TICK)
    except queue.Empty:
        if not thread.is_alive():
            # There is a possibility that after the last queue.get the
            # thread created another event and then died. In that case
            # we want to continue draining the queue.
            try:
                return event_queue.get(block=False)
            except queue.Empty:
                # If the queue empty we know that there are no more events
                # and that the thread has died.
                return THREAD_DEAD_AND_QUEUE_EMPTY

    return None


def execute_thread_step(step):
    """Executes a Step in a new thread.

    Args:
        step (Step): step to execute
    """
    event_queue = queue.Queue()
    thread = threading.Thread(target=_execute_step_in_thread, args=(event_queue, step))
    thread.start()
    completed_properly = False
    while not completed_properly:
        event = _poll_for_event(thread, event_queue)
        if event == THREAD_DEAD_AND_QUEUE_EMPTY:
            break
        yield event
        if isinstance(event, (ThreadDoneEvent, ThreadSystemErrorEvent)):
            completed_properly = True
    if not completed_properly:
        raise ThreadCrashException()
    thread.join()


def execute_pipeline_iterator(pipeline_def):
    yield from MultithreadExecutor().execute(pipeline_def)
