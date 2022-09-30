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
Module contains MultithreadExecutor.

:author: M. Marin (KTH)
:date:   1.11.2020
"""

import os
import sys
from dagster import check
from dagster.core.execution.api import (
    create_execution_plan,
    inner_plan_execution_iterator,
    ExecuteRunWithPlanIterable,
    PlanExecutionContextManager,
)
from dagster.core.executor.base import Executor
from dagster.core.execution.retries import RetryMode
from dagster.core.execution.context.system import PlanOrchestrationContext
from dagster.core.execution.plan.objects import StepFailureData
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.events import DagsterEvent, EngineEventData
from dagster.core.errors import DagsterError
from dagster.utils.timing import format_duration, time_execution_scope
from dagster.utils.error import serializable_error_info_from_exc_info
from .thread_executor import ThreadCrashException, ThreadEvent, ThreadSystemErrorEvent, execute_thread_command
from ..spine_engine import ED

DELEGATE_MARKER = "multithread_thread_init"


class DagsterThreadError(DagsterError):
    """An exception has occurred in one or more of the threads dagster manages.
    This error forwards the message and stack trace for all of the collected errors.
    """

    def __init__(self, *args, **kwargs):
        from dagster.utils.error import SerializableErrorInfo

        self.thread_error_infos = check.list_param(
            kwargs.pop("thread_error_infos"), "thread_error_infos", SerializableErrorInfo
        )
        super(DagsterThreadError, self).__init__(*args, **kwargs)


class MultithreadExecutor(Executor):
    def __init__(self, retries, max_concurrent=None):
        self._retries = check.inst_param(retries, "retries", RetryMode)
        max_concurrent = max_concurrent if max_concurrent else 100  # TODO: How to determine a good amount?
        self.max_concurrent = check.int_param(max_concurrent, "max_concurrent")
        self._forward_resources = {}
        self._backward_resources = {}

    @property
    def retries(self):
        return self._retries

    def execute(self, pipeline_context, execution_plan):
        check.inst_param(pipeline_context, "pipeline_context", PlanOrchestrationContext)
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)

        limit = self.max_concurrent

        yield DagsterEvent.engine_event(
            pipeline_context,
            "Executing steps using multithread executor (pid: {pid})".format(pid=os.getpid()),
            event_specific_data=EngineEventData.in_process(os.getpid(), execution_plan.step_keys_to_execute),
        )

        with time_execution_scope() as timer_result:
            with execution_plan.start(retry_mode=self.retries) as active_execution:
                active_iters = {}
                errors = {}
                waiting = {}
                iterating = {}
                iterating_active = set()
                solid_names_by_jump = pipeline_context.pipeline.get_definition().loops
                jump_by_source = {}
                jump_by_solid_name = {}
                for jump, solid_names in solid_names_by_jump.items():
                    jump_by_source[jump.source] = jump
                    non_nested_solid_names = set(solid_names)
                    for other_jump, other_solid_names in solid_names_by_jump.items():
                        if jump is other_jump:
                            continue
                        if other_solid_names > solid_names:
                            continue
                        non_nested_solid_names -= other_solid_names
                    for solid_name in non_nested_solid_names:
                        jump_by_solid_name[solid_name] = jump
                unfinished_jumps = set(solid_names_by_jump)
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
                            if dependency_keys & (iterating_active | iterating_skipped):
                                iterating_skipped.add(key)
                                continue
                            iterating_active.add(key)
                            candidate_steps.append(step)

                        executable_steps = []
                        for step in candidate_steps:
                            # Check if the step depends on any jumps that don't contain it
                            jumps = (
                                jump
                                for jump in unfinished_jumps
                                if step.solid_name not in solid_names_by_jump.get(jump, ())
                            )
                            solid_names = {item for jump in jumps for item in solid_names_by_jump[jump]}
                            problematic_keys = {
                                key for key, step in steps_by_key.items() if step.solid_name in solid_names
                            }
                            dependency_keys = step.get_execution_dependency_keys()
                            if dependency_keys & problematic_keys:
                                if step.key not in iterating:
                                    waiting[step.key] = step
                                continue
                            waiting.pop(step.key, None)
                            iterating.pop(step.key, None)
                            executable_steps.append(step)

                        if not executable_steps:
                            break

                        for step in executable_steps:
                            step_context = pipeline_context.for_step(step)
                            active_iters[step.key] = self.execute_step_in_thread(
                                pipeline_context.pipeline,
                                step.key,
                                step.solid_name,
                                step_context,
                                errors,
                                active_execution.get_known_state(),
                            )
                    # process active iterators
                    empty_iters = []
                    for key, step_iter in active_iters.items():
                        try:
                            event_or_none = next(step_iter)
                            if event_or_none is None:
                                continue
                            yield event_or_none
                            try:
                                active_execution.handle_event(event_or_none)
                            except check.CheckError:
                                # Bypass check errors on iterating steps
                                if key in iterating_active:
                                    pass
                                else:
                                    raise
                            # Handle loops
                            if event_or_none.is_step_failure:
                                # Mark failed loops as finished
                                iterating_active.discard(key)
                                step = steps_by_key[key]
                                failed_jump = jump_by_solid_name.get(step.solid_name)
                                if failed_jump is None:
                                    continue
                                failed_solid_names = solid_names_by_jump[failed_jump]
                                for jump, solid_names in solid_names_by_jump.items():
                                    if solid_names & failed_solid_names:
                                        unfinished_jumps.discard(jump)
                                        loop_iteration_counters.pop(jump, None)
                            elif event_or_none.is_step_success:
                                # Process loop condition
                                iterating_active.discard(key)
                                step = steps_by_key[key]
                                jump = jump_by_source.get(step.solid_name)
                                if jump is None:
                                    continue
                                forward_resources = self._forward_resources.get(jump.source, [])
                                backward_resources = self._backward_resources.get(jump.destination, [])
                                jump.receive_resources_from_source(forward_resources)
                                jump.receive_resources_from_destination(backward_resources)
                                iteration_counter = loop_iteration_counters.setdefault(jump, 1)
                                if jump.is_condition_true(iteration_counter):
                                    # Put all jump steps in the iterating bucket
                                    for k, s in steps_by_key.items():
                                        if s.solid_name in solid_names_by_jump[jump]:
                                            iterating[k] = s
                                    # Mark all nested jumps unfinished again
                                    for solid_name in solid_names_by_jump[jump]:
                                        nested_jump = jump_by_solid_name.get(solid_name)
                                        if nested_jump is not None:
                                            unfinished_jumps.add(nested_jump)

                                    loop_iteration_counters[jump] += 1
                                else:
                                    unfinished_jumps.remove(jump)
                                    del loop_iteration_counters[jump]

                        except ThreadCrashException:
                            serializable_error = serializable_error_info_from_exc_info(sys.exc_info())
                            yield DagsterEvent.engine_event(
                                pipeline_context,
                                f"Multithread executor: thread for step {key} exited unexpectedly",
                                EngineEventData.engine_error(serializable_error),
                            )
                            step_failure_event = DagsterEvent.step_failure_event(
                                step_context=pipeline_context.for_step(active_execution.get_step_by_key(key)),
                                step_failure_data=StepFailureData(error=serializable_error, user_failure_data=None),
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
                        active_execution.verify_complete(pipeline_context, key)

                    # process skipped and abandoned steps
                    for event in active_execution.plan_events_iterator(pipeline_context):
                        yield event

                errs = {tid: err for tid, err in errors.items() if err}
                if errs:
                    raise DagsterThreadError(
                        "During multithread execution errors occurred in threads:\n{error_list}".format(
                            error_list="\n".join(
                                [
                                    "In thread {tid}: {err}".format(tid=tid, err=err.to_string())
                                    for tid, err in errs.items()
                                ]
                            )
                        ),
                        thread_error_infos=list(errs.values()),
                    )

        yield DagsterEvent.engine_event(
            pipeline_context,
            "Multithread executor: parent process exiting after {duration} (pid: {pid})".format(
                duration=format_duration(timer_result.millis), pid=os.getpid()
            ),
            event_specific_data=EngineEventData.multiprocess(os.getpid()),
        )

    def execute_step_in_thread(self, pipeline, step_key, solid_name, step_context, errors, known_state):
        yield DagsterEvent.engine_event(
            step_context, f"Spawning thread for {step_key}", EngineEventData(marker_start=DELEGATE_MARKER)
        )

        command = ThreadExecutorChildThreadCommand(
            step_context.run_config,
            step_context.pipeline_run,
            step_key,
            step_context.instance,
            pipeline,
            self.retries,
            known_state,
        )
        for ret in execute_thread_command(command):
            if ret is None or isinstance(ret, DagsterEvent):
                self._save_resources(command, solid_name)
                yield ret
            elif isinstance(ret, ThreadEvent):
                if isinstance(ret, ThreadSystemErrorEvent):
                    errors[ret.tid] = ret.error_info
            else:
                check.failed("Unexpected return value from thread {}".format(type(ret)))

    def _save_resources(self, command, solid_name):
        for output_handle, outputs in command.output_capture.items():
            if output_handle.output_name == f"{ED.BACKWARD}_output":
                self._backward_resources[solid_name] = outputs
            elif output_handle.output_name == f"{ED.FORWARD}_output":
                self._forward_resources[solid_name] = [r for stack in outputs for r in stack]


class ThreadExecutorChildThreadCommand:
    def __init__(self, run_config, pipeline_run, step_key, instance, pipeline, retry_mode, known_state):
        self.output_capture = {}
        self._execution_plan = create_execution_plan(
            pipeline=pipeline,
            run_config=run_config,
            mode=pipeline_run.mode,
            step_keys_to_execute=[step_key],
            known_state=known_state,
        )
        self._execution_context_manager = PlanExecutionContextManager(
            pipeline=pipeline,
            retry_mode=retry_mode,
            execution_plan=self._execution_plan,
            run_config=run_config,
            pipeline_run=pipeline_run,
            instance=instance,
            output_capture=self.output_capture,
        )

    def execute(self):
        return iter(
            ExecuteRunWithPlanIterable(
                execution_plan=self._execution_plan,
                iterator=inner_plan_execution_iterator,
                execution_context_manager=self._execution_context_manager,
            )
        )
