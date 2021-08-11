:py:mod:`spine_engine.execution_managers.persistent_execution_manager`
======================================================================

.. py:module:: spine_engine.execution_managers.persistent_execution_manager

.. autoapi-nested-parse::

   Contains PersistentManagerBase, PersistentExecutionManagerBase classes and subclasses,
   as well as some convenience functions.

   :authors: M. Marin (KTH)
   :date:   12.10.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.persistent_execution_manager.PersistentManagerBase
   spine_engine.execution_managers.persistent_execution_manager.JuliaPersistentManager
   spine_engine.execution_managers.persistent_execution_manager.PythonPersistentManager
   spine_engine.execution_managers.persistent_execution_manager._PersistentManagerFactory
   spine_engine.execution_managers.persistent_execution_manager.PersistentExecutionManagerBase
   spine_engine.execution_managers.persistent_execution_manager.JuliaPersistentExecutionManager
   spine_engine.execution_managers.persistent_execution_manager.PythonPersistentExecutionManager



Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.persistent_execution_manager.restart_persistent
   spine_engine.execution_managers.persistent_execution_manager.interrupt_persistent
   spine_engine.execution_managers.persistent_execution_manager.kill_persistent_processes
   spine_engine.execution_managers.persistent_execution_manager.issue_persistent_command
   spine_engine.execution_managers.persistent_execution_manager.get_persistent_completions
   spine_engine.execution_managers.persistent_execution_manager.get_persistent_history_item



Attributes
~~~~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.persistent_execution_manager._persistent_manager_factory


.. py:class:: PersistentManagerBase(args, cwd=None)

   
   :param args: the arguments to launch the persistent process
   :type args: list
   :param cwd: the directory where to start the process
   :type cwd: str, optional

   .. py:method:: language(self)
      :property:

      Returns the underlying language for UI customization in toolbox.

      :returns: str


   .. py:method:: _init_args(self)
      :abstractmethod:

      Returns init args for Popen.

      Subclasses must reimplement to include appropriate switches to ensure the
      process is interactive, or to load modules for internal communication.

      :returns: list


   .. py:method:: _sentinel_command(host, port)
      :staticmethod:
      :abstractmethod:

      Returns a command in the underlying language, that sends a sentinel to a socket listening on given
      host/port.

      Used to synchronize with the persistent process.

      :param host:
      :type host: str
      :param port:
      :type port: int

      :returns: str


   .. py:method:: _start_persistent(self)

      Starts the persistent process.


   .. py:method:: _log_stdout(self)

      Puts stdout from the process into the queue (it will be consumed by issue_command()).


   .. py:method:: _log_stderr(self)

      Puts stderr from the process into the queue (it will be consumed by issue_command()).


   .. py:method:: _is_complete(self, cmd)


   .. py:method:: issue_command(self, cmd, add_history=False)

      Issues cmd to the persistent process and returns an iterator of stdout and stderr messages.

      Each message is a dictionary with two keys:

          - "type": either "stdout" or "stderr"
          - "data": the actual message string.

      :param cmd:
      :type cmd: str

      :returns: generator


   .. py:method:: _make_complete_command(self)


   .. py:method:: _issue_command_and_wait_for_idle(self, cmd, add_history)

      Issues command and wait for idle.

      :param cmd: Command to pass to the persistent process
      :type cmd: str
      :param add_history: Whether or not to add the command to history
      :type add_history: bool


   .. py:method:: _issue_command(self, cmd, is_complete=True, add_history=False)

      Writes command to the process's stdin and flushes.

      :param cmd: Command to pass to the persistent process
      :type cmd: str
      :param is_complete: Whether or not the command is complete
      :type is_complete: bool
      :param add_history: Whether or not to add the command to history
      :type add_history: bool


   .. py:method:: _wait(self)

      Waits for the persistent process to become idle.

      This is implemented by writing the sentinel to stdin and waiting for the _SENTINEL to come out of stdout.

      :returns: bool


   .. py:method:: _listen_and_enqueue(host, port, queue)
      :staticmethod:

      Listens on the server and enqueues all data received.


   .. py:method:: _communicate(self, request, arg, receive=True)

      Sends a request to the persistent process with the given argument.

      :param request: One of the supported requests
      :type request: str
      :param arg: Request argument
      :param receive: If True (the default) also receives the response and returns it.
      :type receive: bool, optional

      :returns: response, or None if the ``receive`` argument is False
      :rtype: str or NoneType


   .. py:method:: get_completions(self, text)

      Returns a list of autocompletion options for given text.

      :param text: Text to complete
      :type text: str

      :returns: List of options
      :rtype: list(str)


   .. py:method:: get_history_item(self, index)

      Returns the history item given by index.

      :param index: Index to retrieve, one-based, 1 means most recent
      :type index: int

      :returns: str


   .. py:method:: restart_persistent(self)

      Restarts the persistent process.


   .. py:method:: interrupt_persistent(self)

      Interrupts the persistent process.


   .. py:method:: _do_interrupt_persistent(self)


   .. py:method:: is_persistent_alive(self)

      Whether or not the persistent is still alive and ready to receive commands.

      :returns: bool


   .. py:method:: kill_process(self)



.. py:class:: JuliaPersistentManager(args, cwd=None)

   Bases: :py:obj:`PersistentManagerBase`

   
   :param args: the arguments to launch the persistent process
   :type args: list
   :param cwd: the directory where to start the process
   :type cwd: str, optional

   .. py:method:: language(self)
      :property:

      See base class.


   .. py:method:: _init_args(self)

      See base class.


   .. py:method:: _sentinel_command(host, port)
      :staticmethod:

      Returns a command in the underlying language, that sends a sentinel to a socket listening on given
      host/port.

      Used to synchronize with the persistent process.

      :param host:
      :type host: str
      :param port:
      :type port: int

      :returns: str



.. py:class:: PythonPersistentManager(args, cwd=None)

   Bases: :py:obj:`PersistentManagerBase`

   
   :param args: the arguments to launch the persistent process
   :type args: list
   :param cwd: the directory where to start the process
   :type cwd: str, optional

   .. py:method:: language(self)
      :property:

      See base class.


   .. py:method:: _init_args(self)

      See base class.


   .. py:method:: _sentinel_command(host, port)
      :staticmethod:

      Returns a command in the underlying language, that sends a sentinel to a socket listening on given
      host/port.

      Used to synchronize with the persistent process.

      :param host:
      :type host: str
      :param port:
      :type port: int

      :returns: str



.. py:class:: _PersistentManagerFactory

   .. py:attribute:: _persistent_managers
      

      Maps tuples (process args) to associated PersistentManagerBase.

   .. py:method:: new_persistent_manager(self, constructor, logger, args, group_id, cwd=None)

      Creates a new persistent for given args and group id if none exists.

      :param constructor: the persistent manager constructor
      :type constructor: function
      :param logger:
      :type logger: LoggerInterface
      :param args: the arguments to launch the persistent process
      :type args: list
      :param group_id: item group that will execute using this persistent
      :type group_id: str
      :param cwd: directory where to start the persistent
      :type cwd: str, optional

      :returns: persistent manager
      :rtype: PersistentManagerBase


   .. py:method:: restart_persistent(self, key)

      Restart a persistent process.

      :param key: persistent identifier
      :type key: tuple


   .. py:method:: interrupt_persistent(self, key)

      Interrupts a persistent process.

      :param key: persistent identifier
      :type key: tuple


   .. py:method:: issue_persistent_command(self, key, cmd)

      Issues a command to a persistent process.

      :param key: persistent identifier
      :type key: tuple
      :param cmd: command to issue
      :type cmd: str

      :returns: stdio and stderr messages (dictionaries with two keys: type, and data)
      :rtype: generator


   .. py:method:: get_persistent_completions(self, key, text)

      Returns a list of completion options.

      :param key: persistent identifier
      :type key: tuple
      :param text: text to complete
      :type text: str

      :returns: options that match given text
      :rtype: list of str


   .. py:method:: get_persistent_history_item(self, key, index)

      Issues a command to a persistent process.

      :param key: persistent identifier
      :type key: tuple
      :param index: index of the history item, most recen first
      :type index: int

      :returns: history item or empty string if none
      :rtype: str


   .. py:method:: kill_manager_processes(self)

      Kills persistent managers' Popen instances.



.. py:data:: _persistent_manager_factory
   

   

.. py:function:: restart_persistent(key)

   See _PersistentManagerFactory.


.. py:function:: interrupt_persistent(key)

   See _PersistentManagerFactory.


.. py:function:: kill_persistent_processes()

   Kills all persistent processes.

   On Windows systems the work directories are reserved by the ``Popen`` objects owned by the persistent managers.
   They need to be killed to before the directories can be modified, e.g. deleted or renamed.


.. py:function:: issue_persistent_command(key, cmd)

   See _PersistentManagerFactory.


.. py:function:: get_persistent_completions(key, text)

   See _PersistentManagerFactory.


.. py:function:: get_persistent_history_item(key, index)

   See _PersistentManagerFactory.


.. py:class:: PersistentExecutionManagerBase(logger, args, commands, alias, group_id=None, workdir=None)

   Bases: :py:obj:`spine_engine.execution_managers.execution_manager_base.ExecutionManagerBase`

   Base class for managing execution of commands on a persistent process.

   Class constructor.

   :param logger: a logger instance
   :type logger: LoggerInterface
   :param args: List of args to start the persistent process
   :type args: list
   :param commands: List of commands to execute in the persistent process
   :type commands: list
   :param group_id: item group that will execute using this kernel
   :type group_id: str, optional
   :param workdir: item group that will execute using this kernel
   :type workdir: str, optional

   .. py:method:: alias(self)
      :property:


   .. py:method:: persistent_manager_factory()
      :staticmethod:
      :abstractmethod:

      Returns a function to create a persistent manager for this execution
      (a subclass of PersistentManagerBase)

      :returns: function


   .. py:method:: run_until_complete(self)

      See base class.


   .. py:method:: stop_execution(self)

      See base class.



.. py:class:: JuliaPersistentExecutionManager(logger, args, commands, alias, group_id=None, workdir=None)

   Bases: :py:obj:`PersistentExecutionManagerBase`

   Manages execution of commands on a Julia persistent process.

   Class constructor.

   :param logger: a logger instance
   :type logger: LoggerInterface
   :param args: List of args to start the persistent process
   :type args: list
   :param commands: List of commands to execute in the persistent process
   :type commands: list
   :param group_id: item group that will execute using this kernel
   :type group_id: str, optional
   :param workdir: item group that will execute using this kernel
   :type workdir: str, optional

   .. py:method:: persistent_manager_factory()
      :staticmethod:

      See base class.



.. py:class:: PythonPersistentExecutionManager(logger, args, commands, alias, group_id=None, workdir=None)

   Bases: :py:obj:`PersistentExecutionManagerBase`

   Manages execution of commands on a Python persistent process.

   Class constructor.

   :param logger: a logger instance
   :type logger: LoggerInterface
   :param args: List of args to start the persistent process
   :type args: list
   :param commands: List of commands to execute in the persistent process
   :type commands: list
   :param group_id: item group that will execute using this kernel
   :type group_id: str, optional
   :param workdir: item group that will execute using this kernel
   :type workdir: str, optional

   .. py:method:: persistent_manager_factory()
      :staticmethod:

      See base class.



