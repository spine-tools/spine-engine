:py:mod:`spine_engine.utils.returning_process`
==============================================

.. py:module:: spine_engine.utils.returning_process

.. autoapi-nested-parse::

   The ReturningProcess class.

   :authors: M. Marin (KTH)
   :date:    3.11.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.utils.returning_process.ReturningProcess




.. py:class:: ReturningProcess(*args, **kwargs)

   Bases: :py:obj:`multiprocessing.Process`

   Process objects represent activity that is run in a separate process

   The class is analogous to `threading.Thread`

   .. py:method:: run_until_complete(self)

      Starts the process and joins it after it has finished.

      :returns: Return value of the process where the first element is a status flag
      :rtype: tuple


   .. py:method:: run(self)

      Method to be run in sub-process; can be overridden in sub-class


   .. py:method:: terminate(self)

      Terminate process; sends SIGTERM signal or uses TerminateProcess()



