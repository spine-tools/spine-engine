:py:mod:`spine_engine.execution_managers.conda_kernel_spec_manager`
===================================================================

.. py:module:: spine_engine.execution_managers.conda_kernel_spec_manager


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.conda_kernel_spec_manager.CondaKernelSpecManager




Attributes
~~~~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.conda_kernel_spec_manager.CACHE_TIMEOUT
   spine_engine.execution_managers.conda_kernel_spec_manager.RUNNER_COMMAND


.. py:data:: CACHE_TIMEOUT
   :annotation: = 60

   

.. py:data:: RUNNER_COMMAND
   :annotation: = ['python', '-m', 'spine_engine.execution_managers.conda_kernel_spec_runner']

   

.. py:class:: CondaKernelSpecManager(**kwargs)

   Bases: :py:obj:`jupyter_client.kernelspec.KernelSpecManager`

   A custom KernelSpecManager able to search for conda environments and
   create kernelspecs for them.

   Create a configurable given a config config.

   :param config: If this is empty, default values are used. If config is a
                  :class:`Config` instance, it will be used to configure the
                  instance.
   :type config: Config
   :param parent: The parent Configurable instance of this object.
   :type parent: Configurable instance, optional

   .. rubric:: Notes

   Subclasses of Configurable must call the :meth:`__init__` method of
   :class:`Configurable` *before* doing anything else and using
   :func:`super`::

       class MyConfigurable(Configurable):
           def __init__(self, config=None):
               super(MyConfigurable, self).__init__(config=config)
               # Then any other code you need to finish initialization.

   This ensures that instances will be configured properly.

   .. py:attribute:: conda_only
      

      

   .. py:attribute:: env_filter
      

      

   .. py:attribute:: kernelspec_path
      

      

   .. py:attribute:: name_format
      

      

   .. py:method:: _validate_kernelspec_path(self, proposal)


   .. py:method:: clean_kernel_name(kname)
      :staticmethod:

      Replaces invalid characters in the Jupyter kernelname, with
      a bit of effort to preserve readability.


   .. py:method:: _conda_info(self)
      :property:

      Get and parse the whole conda information output

      Caches the information for CACHE_TIMEOUT seconds, as this is
      relatively expensive.


   .. py:method:: _all_envs(self)

      Find all of the environments we should be checking. We skip
      environments in the conda-bld directory as well as environments
      that match our env_filter regex. Returns a dict with canonical
      environment names as keys, and full paths as values.


   .. py:method:: _all_specs(self)

      Find the all kernel specs in all environments.

      Returns a dict with unique env names as keys, and the kernel.json
      content as values, modified so that they can be run properly in
      their native environments.

      Caches the information for CACHE_TIMEOUT seconds, as this is
      relatively expensive.


   .. py:method:: _conda_kspecs(self)
      :property:

      Get (or refresh) the cache of conda kernels



   .. py:method:: find_kernel_specs(self)

      Returns a dict mapping kernel names to resource directories.

      The update process also adds the resource dir for the conda
      environments.


   .. py:method:: get_kernel_spec(self, kernel_name)

      Returns a :class:`KernelSpec` instance for the given kernel_name.

      Additionally, conda kernelspecs are generated on the fly
      accordingly with the detected environments.


   .. py:method:: get_all_specs(self)

      Returns a dict mapping kernel names to dictionaries with two
      entries: "resource_dir" and "spec". This was added to fill out
      the full public interface to KernelManagerSpec.


   .. py:method:: remove_kernel_spec(self, name)

      Remove a kernel spec directory by name.

      Returns the path that was deleted.



