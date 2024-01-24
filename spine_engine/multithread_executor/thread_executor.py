######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright (C) 2023-2024 Mopo project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""Facilities for running arbitrary commands in child processes."""

import threading
import queue
import sys
from collections import namedtuple

from dagster import check
from dagster.utils.error import serializable_error_info_from_exc_info
from dagster.core.execution.plan.execute_plan import _dagster_event_sequence_for_step
from dagster.core.events import DagsterEvent


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


def _execute_command_in_thread(event_queue, command):
    """Wraps the execution of a command.

    Handles errors and communicates across a queue with the parent thread.

    Args:
        event_queue (Queue): event queue
        command (ChildThreadCommand): execution command
    """
    tid = threading.get_ident()
    event_queue.put(ThreadStartEvent(tid=tid))
    try:
        for step_event in command.execute():
            check.inst(step_event, DagsterEvent)
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


def execute_thread_command(command):
    """Executes a ChildThreadCommand in a new thread.

    Args:
        command (ChildThreadCommand): command to execute
    """
    event_queue = queue.Queue()

    thread = threading.Thread(target=_execute_command_in_thread, args=(event_queue, command))

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
