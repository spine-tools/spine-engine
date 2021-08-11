:py:mod:`spine_engine.multithread_executor.executor`
====================================================

.. py:module:: spine_engine.multithread_executor.executor

.. autoapi-nested-parse::

   Module contains multithread_executor.

   :author: M. Marin (KTH)
   :date:   1.11.2020



Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.multithread_executor.executor.multithread_executor



.. py:function:: multithread_executor(init_context)

   A custom multithread executor.

   This simple multithread executor borrows almost all the code from dagster's builtin multiprocess executor,
   but takes a twist to use threading instead of multiprocessing.
   To select the multithread executor, include a fragment
   such as the following in your config:

   .. code-block:: yaml

       execution:
         multithread:
           config:
               max_concurrent: 4

   The ``max_concurrent`` arg is optional and tells the execution engine how many threads may run
   concurrently. By default, or if you set ``max_concurrent`` to be 0, this is 100
   (in attendance of a better method).

   Execution priority can be configured using the ``dagster/priority`` tag via solid metadata,
   where the higher the number the higher the priority. 0 is the default and both positive
   and negative numbers can be used.


