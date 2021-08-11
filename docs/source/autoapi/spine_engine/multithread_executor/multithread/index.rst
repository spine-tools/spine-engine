:py:mod:`spine_engine.multithread_executor.multithread`
=======================================================

.. py:module:: spine_engine.multithread_executor.multithread

.. autoapi-nested-parse::

   Module contains MultithreadExecutor.

   :author: M. Marin (KTH)
   :date:   1.11.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.multithread_executor.multithread.MultithreadExecutor




Attributes
~~~~~~~~~~

.. autoapisummary::

   spine_engine.multithread_executor.multithread.DELEGATE_MARKER


.. py:data:: DELEGATE_MARKER
   :annotation: = multithread_threat_init

   

.. py:exception:: DagsterThreadError(*args, **kwargs)

   Bases: :py:obj:`dagster.core.errors.DagsterError`

   An exception has occurred in one or more of the threads dagster manages.
   This error forwards the message and stack trace for all of the collected errors.

   Initialize self.  See help(type(self)) for accurate signature.


.. py:class:: MultithreadExecutor(retries, max_concurrent=None)

   Bases: :py:obj:`dagster.core.executor.base.Executor`

   Helper class that provides a standard way to create an ABC using
   inheritance.

   .. py:method:: retries(self)
      :property:

      The Retries state / policy for this instance of the Executor. Executors should allow this to be
      controlled via configuration if possible.

      Returns: Retries


   .. py:method:: execute(self, pipeline_context, execution_plan)

      For the given context and execution plan, orchestrate a series of sub plan executions in a way that satisfies the whole plan being executed.

      :param pipeline_context: The pipeline execution context.
      :type pipeline_context: SystemPipelineExecutionContext
      :param execution_plan: The plan to execute.
      :type execution_plan: ExecutionPlan

      :returns: A stream of dagster events.


   .. py:method:: execute_step_in_thread(self, step_key, step_context, errors)



