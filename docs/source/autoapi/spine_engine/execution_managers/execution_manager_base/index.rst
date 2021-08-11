:py:mod:`spine_engine.execution_managers.execution_manager_base`
================================================================

.. py:module:: spine_engine.execution_managers.execution_manager_base

.. autoapi-nested-parse::

   Contains the ExecutionManagerBase class.

   :authors: M. Marin (KTH)
   :date:   12.10.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.execution_manager_base.ExecutionManagerBase




.. py:class:: ExecutionManagerBase(logger)

   Base class for all tool instance execution managers.

   Class constructor.

   :param logger: a logger instance
   :type logger: LoggerInterface

   .. py:method:: run_until_complete(self)
      :abstractmethod:

      Runs until completion.

      :returns: return code
      :rtype: int


   .. py:method:: stop_execution(self)
      :abstractmethod:

      Stops execution gracefully.



