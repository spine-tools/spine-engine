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
import threading
from dagster import check
from dagster.core.executor.base import Executor
from dagster.core.execution.retries import Retries
from dagster.core.execution.context.system import SystemPipelineExecutionContext
from dagster.core.execution.plan.objects import StepFailureData
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.events import DagsterEvent, EngineEventData
from dagster.core.errors import DagsterError
from dagster.utils.timing import format_duration, time_execution_scope
from dagster.utils.error import serializable_error_info_from_exc_info


from .thread_executor import ThreadCrashException, ThreadEvent, ThreadSystemErrorEvent, execute_thread_step

DELEGATE_MARKER = "multithread_threat_init"


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
        self._retries = check.inst_param(retries, "retries", Retries)
        max_concurrent = max_concurrent if max_concurrent else 100  # TODO: How to determine a good amount?
        self.max_concurrent = check.int_param(max_concurrent, "max_concurrent")

    @property
    def retries(self):
        return self._retries

    def execute(self, pipeline_context, execution_plan):
        check.inst_param(pipeline_context, "pipeline_context", SystemPipelineExecutionContext)
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)

        limit = self.max_concurrent

        yield DagsterEvent.engine_event(
            pipeline_context,
            "Executing steps using multithread executor (pid: {pid})".format(pid=os.getpid()),
            event_specific_data=EngineEventData.in_process(os.getpid(), execution_plan.step_keys_to_execute),
        )

        with time_execution_scope() as timer_result:
            with execution_plan.start(retries=self.retries) as active_execution:
                active_iters = {}
                errors = {}

                while not active_execution.is_complete or active_iters:

                    # start iterators
                    while len(active_iters) < limit:
                        steps = active_execution.get_steps_to_execute(limit=(limit - len(active_iters)))

                        if not steps:
                            break

                        for step in steps:
                            step_context = pipeline_context.for_step(step)
                            active_iters[step.key] = self.execute_step_in_thread(step.key, step_context, errors)

                    # process active iterators
                    empty_iters = []
                    for key, step_iter in active_iters.items():
                        try:
                            event_or_none = next(step_iter)
                            if event_or_none is None:
                                continue
                            yield event_or_none
                            active_execution.handle_event(event_or_none)

                        except ThreadCrashException:
                            serializable_error = serializable_error_info_from_exc_info(sys.exc_info())
                            yield DagsterEvent.engine_event(
                                pipeline_context,
                                f"Multithread executor: thread for step {key} exited unexpectedly",
                                EngineEventData.engine_error(serializable_error),
                                step_key=key,
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

    def execute_step_in_thread(self, step_key, step_context, errors):
        yield DagsterEvent.engine_event(
            step_context,
            "Spawning thread for {}".format(step_key),
            EngineEventData(marker_start=DELEGATE_MARKER),
            step_key=step_key,
        )

        for ret in execute_thread_step(step_context, self.retries):
            if ret is None or isinstance(ret, DagsterEvent):
                yield ret
            elif isinstance(ret, ThreadEvent):
                if isinstance(ret, ThreadSystemErrorEvent):
                    errors[ret.tid] = ret.error_info
            else:
                check.failed("Unexpected return value from thread {}".format(type(ret)))
