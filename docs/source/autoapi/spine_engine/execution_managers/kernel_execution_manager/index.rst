:py:mod:`spine_engine.execution_managers.kernel_execution_manager`
==================================================================

.. py:module:: spine_engine.execution_managers.kernel_execution_manager

.. autoapi-nested-parse::

   Contains the KernelExecutionManager class and subclasses, and some convenience functions.

   :authors: M. Marin (KTH)
   :date:   12.10.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.kernel_execution_manager._KernelManagerFactory
   spine_engine.execution_managers.kernel_execution_manager.KernelExecutionManager



Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.kernel_execution_manager.get_kernel_manager
   spine_engine.execution_managers.kernel_execution_manager.pop_kernel_manager



Attributes
~~~~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.kernel_execution_manager._kernel_manager_factory


.. py:class:: _KernelManagerFactory

   .. py:attribute:: _kernel_managers
      

      Maps tuples (kernel name, group id) to associated KernelManager.

   .. py:attribute:: _key_by_connection_file
      

      Maps connection file string to tuple (kernel_name, group_id). Mostly for fast lookup in ``restart_kernel()``

   .. py:method:: _make_kernel_manager(self, kernel_name, group_id)

      Creates a new kernel manager for given kernel and group id if none exists, and returns it.

      :param kernel_name: the kernel
      :type kernel_name: str
      :param group_id: item group that will execute using this kernel
      :type group_id: str

      :returns: KernelManager


   .. py:method:: new_kernel_manager(self, kernel_name, group_id, logger, extra_switches=None, environment='', **kwargs)

      Creates a new kernel manager for given kernel and group id if none exists.
      Starts the kernel if not started, and returns it.

      :param kernel_name: the kernel
      :type kernel_name: str
      :param group_id: item group that will execute using this kernel
      :type group_id: str
      :param logger: for logging
      :type logger: LoggerInterface
      :param extra_switches: List of additional switches to julia or python.
                             These come before the 'programfile'.
      :type extra_switches: list, optional
      :param environment: "conda" to launch a Conda kernel spec. "" for a regular kernel spec
      :type environment: str
      :param `**kwargs`: optional. Keyword arguments passed to ``KernelManager.start_kernel()``

      :returns: KernelManager


   .. py:method:: get_kernel_manager(self, connection_file)

      Returns a kernel manager for given connection file if any.

      :param connection_file: path of connection file
      :type connection_file: str

      :returns: KernelManager or None


   .. py:method:: pop_kernel_manager(self, connection_file)

      Returns a kernel manager for given connection file if any.
      It also removes it from cache.

      :param connection_file: path of connection file
      :type connection_file: str

      :returns: KernelManager or None



.. py:data:: _kernel_manager_factory
   

   

.. py:function:: get_kernel_manager(connection_file)


.. py:function:: pop_kernel_manager(connection_file)


.. py:class:: KernelExecutionManager(logger, kernel_name, *commands, group_id=None, workdir=None, startup_timeout=60, extra_switches=None, environment='', **kwargs)

   Bases: :py:obj:`spine_engine.execution_managers.execution_manager_base.ExecutionManagerBase`

   Base class for all tool instance execution managers.

   :param logger:
   :type logger: LoggerInterface
   :param kernel_name: the kernel
   :type kernel_name: str
   :param \*commands: Commands to execute in the kernel
   :param group_id: item group that will execute using this kernel
   :type group_id: str, optional
   :param workdir: item group that will execute using this kernel
   :type workdir: str, optional
   :param startup_timeout: How much to wait for the kernel, used in ``KernelClient.wait_for_ready()``
   :type startup_timeout: int, optional
   :param extra_switches: List of additional switches to launch julia.
                          These come before the 'programfile'.
   :type extra_switches: list, optional
   :param environment: "conda" to launch a Conda kernel spec. "" for a regular kernel spec.
   :type environment: str
   :param \*\*kwargs: Keyword arguments passed to ``KernelManager.start_kernel()``
   :type \*\*kwargs: optional

   .. py:method:: run_until_complete(self)

      Runs until completion.

      :returns: return code
      :rtype: int


   .. py:method:: _do_run(self)


   .. py:method:: stop_execution(self)

      Stops execution gracefully.



