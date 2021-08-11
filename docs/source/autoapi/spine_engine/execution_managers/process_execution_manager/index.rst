:py:mod:`spine_engine.execution_managers.process_execution_manager`
===================================================================

.. py:module:: spine_engine.execution_managers.process_execution_manager

.. autoapi-nested-parse::

   Contains the ProcessExecutionManager class.

   :authors: M. Marin (KTH)
   :date:   12.10.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.process_execution_manager.ProcessExecutionManager




.. py:class:: ProcessExecutionManager(logger, program, *args, workdir=None)

   Bases: :py:obj:`spine_engine.execution_managers.execution_manager_base.ExecutionManagerBase`

   Base class for all tool instance execution managers.

   Class constructor.

   :param logger: a logger instance
   :type logger: LoggerInterface
   :param program: Path to program to run in the subprocess (e.g. julia.exe)
   :type program: str
   :param args: List of argument for the program (e.g. path to script file)
   :type args: list

   .. py:method:: run_until_complete(self)

      Runs until completion.

      :returns: return code
      :rtype: int


   .. py:method:: stop_execution(self)

      Stops execution gracefully.


   .. py:method:: _log_stdout(self, stdout)


   .. py:method:: _log_stderr(self, stderr)



