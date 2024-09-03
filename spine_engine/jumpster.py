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
from collections import namedtuple
from enum import Enum, unique
import queue
import sys
import threading
from .utils.helpers import ExecutionDirection as ED
from .utils.helpers import serializable_error_info_from_exc_info


@unique
class JumpsterEventType(Enum):
    STEP_START = 1
    STEP_FAILURE = 2
    STEP_FINISH = 3
    OUTPUT_AVAILABLE = 4


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
        for key, value in kwargs.items():
            setattr(self, key, value)


class Failure(BaseException):
    """Item execution stopped by failure or user interruption."""


class Output:
    def __init__(self, value):
        self.value = value


class Finalization:
    def __init__(self, item_finish_state):
        self.item_finish_state = item_finish_state


class JumpsterThreadError(Exception):
    """An exception has occurred in one or more of the threads jumpster manages.
    This error forwards the message and stack trace for all the collected errors.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.thread_error_infos = kwargs.pop("thread_error_infos")


class MultithreadExecutor:
    def __init__(self, pipeline_def, max_concurrent=None):
        self._pipeline_def = pipeline_def
        self._max_concurrent = max_concurrent if max_concurrent else 100  # TODO: How to determine a good amount?
        self._output_value = {}
        self._ready_to_execute = {}
        self._in_flight = set()
        self._steps_by_input_key = {}
        for solid_def in self._pipeline_def.solid_defs:
            step = Step(solid_def)
            if step.is_ready_to_execute():
                self._ready_to_execute[step.key] = step
            else:
                for input_def in solid_def.input_defs:
                    self._steps_by_input_key.setdefault(input_def.key, []).append(step)

    def _is_complete(self):
        return not self._in_flight and not self._ready_to_execute

    def _get_steps_to_execute(self, limit):
        steps = []
        while self._ready_to_execute and len(steps) < limit:
            key, step = self._ready_to_execute.popitem()
            self._in_flight.add(key)
            steps.append(step)
        return steps

    def _handle_event(self, event):
        if event.event_type in (JumpsterEventType.STEP_FINISH, JumpsterEventType.STEP_FAILURE):
            self._in_flight.discard(event.key)
        elif event.event_type == JumpsterEventType.OUTPUT_AVAILABLE:
            self._output_value[event.key] = event.output_value
            steps = self._steps_by_input_key.get(event.key, ())
            for step in steps:
                step.inputs[event.key] = event.output_value
                if step.is_ready_to_execute():
                    self._ready_to_execute[step.key] = step

    def execute(self):
        active_iters = {}
        errors = {}
        waiting = {}
        iterating = {}
        iterating_active = set()
        iterating_failed = set()
        jumps = self._pipeline_def.jumps
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
        step_by_key = {}
        while not self._is_complete() or active_iters:
            # start iterators
            while len(active_iters) < self._max_concurrent:
                candidate_steps = self._get_steps_to_execute(limit=(self._max_concurrent - len(active_iters)))
                step_by_key.update({step.key: step for step in candidate_steps})
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
                        for k, s in step_by_key.items()
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
                    self._handle_event(event_or_none)
                    step = step_by_key[key]
                    if step.direction == ED.BACKWARD:
                        continue
                    # Handle loops
                    if event_or_none.event_type == JumpsterEventType.STEP_FAILURE:
                        # Mark failed loops as finished
                        iterating_active.discard(key)
                        failed_jump = jump_by_item_name.get(step.item_name)
                        if failed_jump is None:
                            continue
                        failed_item_names = failed_jump.item_names
                        for jump in jumps:
                            if jump.item_names & failed_item_names:
                                unfinished_jumps.discard(jump)
                                loop_iteration_counters.pop(jump, None)
                        iterating_failed.add(step)
                    elif event_or_none.event_type == JumpsterEventType.STEP_FINISH:
                        # Process loop condition
                        iterating_active.discard(key)
                        jump = jump_by_source.get(step.item_name)
                        if jump is None:
                            continue
                        forward_resources = [
                            r for stack in self._output_value.get((jump.source, ED.FORWARD), []) for r in stack
                        ]
                        backward_resources = self._output_value.get((jump.destination, ED.BACKWARD), [])
                        jump.receive_resources_from_source(forward_resources)
                        jump.receive_resources_from_destination(backward_resources)
                        iteration_counter = loop_iteration_counters.setdefault(jump, 1)
                        if jump.is_condition_true(iteration_counter):
                            # Put all jump steps in the iterating bucket
                            for k, s in step_by_key.items():
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
                    step_failure_event = JumpsterEvent(JumpsterEventType.STEP_FAILURE, *key, error=serializable_error)
                    self._handle_event(step_failure_event)
                    yield step_failure_event
                    empty_iters.append(key)
                except StopIteration:
                    empty_iters.append(key)
            # clear and mark complete finished iterators
            for key in empty_iters:
                del active_iters[key]
        errs = {tid: err for tid, err in errors.items() if err}
        if errs:
            raise JumpsterThreadError(
                "During multithread execution errors occurred in threads:\n{error_list}".format(
                    error_list="\n".join([f"In thread {tid}: {err.to_string()}" for tid, err in errs.items()])
                ),
                thread_error_infos=list(errs.values()),
            )


class Step:
    def __init__(self, solid_def):
        self._solid_def = solid_def
        self.inputs = {}
        self._ready_once = False

    @property
    def item_name(self):
        return self._solid_def.item_name

    @property
    def direction(self):
        return self._solid_def.direction

    @property
    def key(self):
        return self._solid_def.key

    def is_ready_to_execute(self):
        if self._ready_once:
            return False
        self._ready_once = len(self._solid_def.input_defs) == len(self.inputs)
        return self._ready_once

    def execute(self):
        inputs = {}
        for input_def in self._solid_def.input_defs:
            inputs.setdefault(input_def.direction, []).extend(self.inputs[input_def.key])
        yield JumpsterEvent(JumpsterEventType.STEP_START, *self.key)
        try:
            for x in self._solid_def.compute_fn(inputs):
                if isinstance(x, Output):
                    yield JumpsterEvent(JumpsterEventType.OUTPUT_AVAILABLE, *self.key, output_value=x.value)
                elif isinstance(x, Finalization):
                    yield JumpsterEvent(JumpsterEventType.STEP_FINISH, *self.key, item_finish_state=x.item_finish_state)
        except Exception as err:  # pylint: disable=broad-except
            yield JumpsterEvent(JumpsterEventType.STEP_FAILURE, *self.key, error=err)

    def get_execution_dependency_keys(self):
        return set(input_def.key for input_def in self._solid_def.input_defs)


def execute_pipeline_iterator(pipeline_def):
    yield from MultithreadExecutor(pipeline_def).execute()


# Thread execution stuff
class ThreadDoneEvent(namedtuple("ThreadDoneEvent", "tid")):
    pass


class ThreadSystemErrorEvent(namedtuple("ThreadSystemErrorEvent", "tid error_info")):
    pass


class ThreadCrashException(Exception):
    """Thrown when the thread crashes."""


TICK = 20.0 * 1.0 / 1000.0
"""The minimum interval at which to check for child process liveness -- default 20ms."""

THREAD_DEAD_AND_QUEUE_EMPTY = "THREAD_DEAD_AND_QUEUE_EMPTY"
"""Sentinel value."""


def execute_step_in_thread(step, errors):
    """Executes a Step in a new thread.

    Args:
        step (Step): step to execute
        errors (dict): mapping from thread id to error info
    """
    event_queue = queue.Queue()
    thread = threading.Thread(target=_do_execute_step_in_thread, args=(event_queue, step))
    thread.start()
    completed_properly = False
    while not completed_properly:
        event = _poll_for_event(thread, event_queue)
        if event == THREAD_DEAD_AND_QUEUE_EMPTY:
            break
        if event is None or isinstance(event, JumpsterEvent):
            yield event
        elif isinstance(event, (ThreadDoneEvent, ThreadSystemErrorEvent)):
            completed_properly = True
            if isinstance(event, ThreadSystemErrorEvent):
                errors[event.tid] = event.error_info
    if not completed_properly:
        raise ThreadCrashException()
    thread.join()


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


def _do_execute_step_in_thread(event_queue, step):
    """Wraps the execution of a step.

    Handles errors and communicates across a queue with the parent thread.

    Args:
        event_queue (Queue): event queue
        step (Step): execution step
    """
    tid = threading.get_ident()
    try:
        for step_event in step.execute():
            event_queue.put(step_event)
        event_queue.put(ThreadDoneEvent(tid=tid))
    except Exception:  # pylint: disable=broad-except
        event_queue.put(
            ThreadSystemErrorEvent(tid=tid, error_info=serializable_error_info_from_exc_info(sys.exc_info()))
        )
    except Failure:
        event_queue.put(JumpsterEvent(JumpsterEventType.STEP_FAILURE, step.item_name, step.direction))
