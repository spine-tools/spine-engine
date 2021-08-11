:py:mod:`spine_engine.multithread_executor.thread_executor`
===========================================================

.. py:module:: spine_engine.multithread_executor.thread_executor

.. autoapi-nested-parse::

   Facilities for running arbitrary commands in child processes.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.multithread_executor.thread_executor.ThreadEvent
   spine_engine.multithread_executor.thread_executor.ThreadStartEvent
   spine_engine.multithread_executor.thread_executor.ThreadDoneEvent
   spine_engine.multithread_executor.thread_executor.ThreadSystemErrorEvent



Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.multithread_executor.thread_executor._execute_step_in_thread
   spine_engine.multithread_executor.thread_executor._poll_for_event
   spine_engine.multithread_executor.thread_executor.execute_thread_step



Attributes
~~~~~~~~~~

.. autoapisummary::

   spine_engine.multithread_executor.thread_executor.TICK
   spine_engine.multithread_executor.thread_executor.THREAD_DEAD_AND_QUEUE_EMPTY


.. py:class:: ThreadEvent


.. py:class:: ThreadStartEvent

   Bases: :py:obj:`namedtuple`\ (\ :py:obj:`'ThreadStartEvent'`\ , :py:obj:`'tid'`\ ), :py:obj:`ThreadEvent`

   docstring

   Initialize self.  See help(type(self)) for accurate signature.


.. py:class:: ThreadDoneEvent

   Bases: :py:obj:`namedtuple`\ (\ :py:obj:`'ThreadDoneEvent'`\ , :py:obj:`'tid'`\ ), :py:obj:`ThreadEvent`

   docstring

   Initialize self.  See help(type(self)) for accurate signature.


.. py:class:: ThreadSystemErrorEvent

   Bases: :py:obj:`namedtuple`\ (\ :py:obj:`'ThreadSystemErrorEvent'`\ , :py:obj:`'tid error_info'`\ ), :py:obj:`ThreadEvent`

   docstring

   Initialize self.  See help(type(self)) for accurate signature.


.. py:exception:: ThreadCrashException

   Bases: :py:obj:`Exception`

   Thrown when the thread crashes.

   Initialize self.  See help(type(self)) for accurate signature.


.. py:function:: _execute_step_in_thread(event_queue, step_context, retries)

   Wraps the execution of a step.

   Handles errors and communicates across a queue with the parent process.


.. py:data:: TICK
   

   The minimum interval at which to check for child process liveness -- default 20ms.

.. py:data:: THREAD_DEAD_AND_QUEUE_EMPTY
   :annotation: = THREAD_DEAD_AND_QUEUE_EMPTY

   Sentinel value.

.. py:function:: _poll_for_event(thread, event_queue)


.. py:function:: execute_thread_step(step_context, retries)

   Execute a step in a new thread.

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

   :param step_context: The step context to execute in the child process.
   :type step_context: SystemStepExecutionContext
   :param retries:
   :type retries: Retries

   Warning: if the thread is in an infinite loop, this will
   also infinitely loop.


