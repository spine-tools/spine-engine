:py:mod:`spine_engine.utils.helpers`
====================================

.. py:module:: spine_engine.utils.helpers

.. autoapi-nested-parse::

   Helpers functions and classes.

   :authors: M. Marin (KTH)
   :date:   20.11.2019



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.utils.helpers.Singleton
   spine_engine.utils.helpers.AppSettings



Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.utils.helpers.shorten
   spine_engine.utils.helpers.create_log_file_timestamp
   spine_engine.utils.helpers.create_timestamp
   spine_engine.utils.helpers.resolve_conda_executable
   spine_engine.utils.helpers.resolve_python_interpreter
   spine_engine.utils.helpers.resolve_julia_executable
   spine_engine.utils.helpers.resolve_gams_executable
   spine_engine.utils.helpers.resolve_executable_from_path
   spine_engine.utils.helpers.inverted
   spine_engine.utils.helpers.get_julia_command
   spine_engine.utils.helpers.get_julia_env



.. py:class:: Singleton

   Bases: :py:obj:`type`

   .. py:attribute:: _instances
      

      

   .. py:method:: __call__(cls, *args, **kwargs)

      Call self as a function.



.. py:class:: AppSettings(settings)

   A QSettings replacement.

   Init.

   :param settings:
   :type settings: dict

   .. py:method:: value(self, key, defaultValue='')



.. py:function:: shorten(name)

   Returns the 'short name' version of given name.


.. py:function:: create_log_file_timestamp()

   Creates a new timestamp string that is used as Data Store and Importer error log file.

   :returns: Timestamp string or empty string if failed.


.. py:function:: create_timestamp()


.. py:function:: resolve_conda_executable(conda_path)

   If given conda_path is an empty str, returns current Conda
   executable from CONDA_EXE env variable if the app was started
   on Conda, otherwise returns an empty string.


.. py:function:: resolve_python_interpreter(python_path)

   If given python_path is empty, returns the
   full path to Python interpreter depending on user's
   settings and whether the app is frozen or not.


.. py:function:: resolve_julia_executable(julia_path)

   if given julia_path is empty, tries to find the path to Julia
   in user's PATH env variable. If Julia is not found in PATH,
   returns an empty string.

   Note: In the long run, we should decide whether this is something we want to do
   because adding julia-x.x./bin/ dir to the PATH is not recommended because this
   also exposes some .dlls to other programs on user's (windows) system. I.e. it
   may break other programs, and this is why the Julia installer does not
   add (and does not even offer the chance to add) Julia to PATH.


.. py:function:: resolve_gams_executable(gams_path)

   if given gams_path is empty, tries to find the path to Gams
   in user's PATH env variable. If Gams is not found in PATH,
   returns an empty string.


.. py:function:: resolve_executable_from_path(executable_name)

   Returns full path to executable name in user's
   PATH env variable. If not found, returns an empty string.

   Basically equivalent to 'where' and 'which' commands in
   cmd.exe and bash respectively.

   :param executable_name: Executable filename to find (e.g. python.exe, julia.exe)
   :type executable_name: str

   :returns: Full path or empty string
   :rtype: str


.. py:function:: inverted(input_)

   Inverts a dictionary of list values.

   :param input_:
   :type input_: dict

   :returns: keys are list items, and values are keys listing that item from the input dictionary
   :rtype: dict


.. py:function:: get_julia_command(settings)

   :param settings:
   :type settings: QSettings, AppSettings

   :returns: e.g. ["path/to/julia", "--project=path/to/project/"]
   :rtype: list


.. py:function:: get_julia_env(settings)

   :param settings:
   :type settings: QSettings, AppSettings

   :returns: (julia_exe, julia_project), or None if none found
   :rtype: tuple, NoneType


