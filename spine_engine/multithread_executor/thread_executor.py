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


def _execute_step_in_thread(event_queue, step_context, retries):
    """Wraps the execution of a step.

    Handles errors and communicates across a queue with the parent process."""

    tid = threading.get_ident()
    event_queue.put(ThreadStartEvent(tid=tid))
    try:
        for step_event in check.generator(_dagster_event_sequence_for_step(step_context, retries)):
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


def execute_thread_step(step_context, retries):
    """Execute a step in a new thread.

    This function starts a new thread whose execution target is the given step context wrapped by
    _execute_step_in_thread; polls the queue for events yielded by the thread
    until it dies and the queue is empty.

    This function yields a complex set of objects to enable having multiple thread
    executions in flight:
        * None - nothing has happened, yielded to enable cooperative multitasking other iterators

        * ThreadEvent - Family of objects that communicates state changes in the thread

        * KeyboardInterrupt - Yielded in the case that an interrupt was recieved while
            polling the thread. Yielded instead of raised to allow forwarding of the
            interrupt to the thread and completion of the iterator for this thread and
            any others that may be executing

        * The actual values yielded by the thread execution

    Args:
        step_context (SystemStepExecutionContext): The step context to execute in the child process.
        retries (Retries)

    Warning: if the thread is in an infinite loop, this will
    also infinitely loop.
    """

    event_queue = queue.Queue()

    thread = threading.Thread(target=_execute_step_in_thread, args=(event_queue, step_context, retries))

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
        # TODO Figure out what to do about stderr/stdout
        raise ThreadCrashException()

    thread.join()
