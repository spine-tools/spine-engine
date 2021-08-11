:py:mod:`spine_engine`
======================

.. py:module:: spine_engine


Subpackages
-----------
.. toctree::
   :titlesonly:
   :maxdepth: 3

   execution_managers/index.rst
   multithread_executor/index.rst
   project_item/index.rst
   utils/index.rst


Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   config/index.rst
   load_project_items/index.rst
   project_item_loader/index.rst
   spine_engine/index.rst
   spine_engine_server/index.rst
   version/index.rst


Package Contents
----------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.ExecutionDirection
   spine_engine.SpineEngine
   spine_engine.SpineEngineState
   spine_engine.ItemExecutionFinishState




Attributes
~~~~~~~~~~

.. autoapisummary::

   spine_engine.__version__


.. py:class:: ExecutionDirection

   Bases: :py:obj:`enum.Enum`

   Generic enumeration.

   Derive from this class to define new enumerations.

   .. py:attribute:: FORWARD
      

      

   .. py:attribute:: BACKWARD
      

      

   .. py:method:: __str__(self)

      Return str(self).



.. py:class:: SpineEngine(items=None, specifications=None, connections=None, items_module_name='spine_items', settings=None, project_dir=None, execution_permits=None, node_successors=None, debug=False)

   An engine for executing a Spine Toolbox DAG-workflow.

   :param items: List of executable item dicts.
   :type items: list(dict)
   :param specifications: A mapping from item type to list of specification dicts.
   :type specifications: dict(str,list(dict))
   :param connections: List of connection dicts
   :type connections: list of dict
   :param items_module_name: name of the Python module that contains project items
   :type items_module_name: str
   :param settings: Toolbox execution settings.
   :type settings: dict
   :param project_dir: Path to project directory.
   :type project_dir: str
   :param execution_permits: A mapping from item name to a boolean value, False indicating that
                             the item is not executed, only its resources are collected.
   :type execution_permits: dict(str,bool)
   :param node_successors: A mapping from item name to list of successor item names, dictating the dependencies.
   :type node_successors: dict(str,list(str))
   :param debug: Whether debug mode is active or not.
   :type debug: bool

   .. py:method:: _make_item_specifications(self, specifications, project_item_loader, items_module_name)

      Instantiates item specifications.

      :param specifications: A mapping from item type to list of specification dicts.
      :type specifications: dict
      :param project_item_loader: loader instance
      :type project_item_loader: ProjectItemLoader
      :param items_module_name: name of the Python module that contains the project items
      :type items_module_name: str

      :returns: mapping from item type to a dict that maps specification names to specification instances
      :rtype: dict


   .. py:method:: _make_item(self, item_name, direction)

      Recreates item from project item dictionary. Note that all items are created twice.
      One for the backward pipeline, the other one for the forward pipeline.


   .. py:method:: get_event(self)

      Returns the next event in the stream. Calling this after receiving the event of type "dag_exec_finished"
      will raise StopIterationError.


   .. py:method:: state(self)


   .. py:method:: _get_event_stream(self)

      Returns an iterator of tuples (event_type, event_data).

      TODO: Describe the events in depth.


   .. py:method:: answer_prompt(self, item_name, accepted)


   .. py:method:: run(self)

      Runs this engine.


   .. py:method:: _process_event(self, event)

      Processes events from a pipeline.

      :param event: an event
      :type event: DagsterEvent


   .. py:method:: stop(self)

      Stops the engine.


   .. py:method:: _stop_item(self, item)


   .. py:method:: _make_pipeline(self)

      Returns a PipelineDefinition for executing this engine.

      :returns: PipelineDefinition


   .. py:method:: _make_backward_solid_def(self, item_name)

      Returns a SolidDefinition for executing the given item in the backward sweep.

      :param item_name: The project item that gets executed by the solid.
      :type item_name: str


   .. py:method:: _make_forward_solid_def(self, item_name)

      Returns a SolidDefinition for executing the given item.

      :param item_name:
      :type item_name: str

      :returns: SolidDefinition


   .. py:method:: _execute_item(self, context, item_name, forward_resource_stacks, backward_resources)

      Executes the given item using the given forward resource stacks and backward resources.
      Returns list of output resource stacks.

      Called by ``_make_forward_solid_def.compute_fn``.

      For each element yielded by ``_filtered_resources_iterator``, spawns a thread that runs ``_execute_item_filtered``.

      :param context:
      :param item_name:
      :type item_name: str
      :param forward_resource_stacks:
      :type forward_resource_stacks: list(tuple(ProjectItemResource))
      :param backward_resources:
      :type backward_resources: list(ProjectItemResource)

      :returns: list(tuple(ProjectItemResource))


   .. py:method:: _execute_item_filtered(self, item, filtered_forward_resources, filtered_backward_resources, output_resources_list, success)

      Executes the given item using the given filtered resources. Target for threads in ``_execute_item``.

      :param item:
      :type item: ExecutableItemBase
      :param filtered_forward_resources:
      :type filtered_forward_resources: list(ProjectItemResource)
      :param filtered_backward_resources:
      :type filtered_backward_resources: list(ProjectItemResource)
      :param output_resources_list: A list of lists, to append the
                                    output resources generated by the item.
      :type output_resources_list: list(list(ProjectItemResource))
      :param success: A list of one element, to write the outcome of the execution.
      :type success: list


   .. py:method:: _filtered_resources_iterator(self, item_name, forward_resource_stacks, backward_resources, timestamp)

      Yields tuples of (filtered forward resources, filtered backward resources, filter id).

      Each tuple corresponds to a unique filter combination. Combinations are obtained by applying the cross-product
      over forward resource stacks as yielded by ``_forward_resource_stacks_iterator``.

      :param item_name:
      :type item_name: str
      :param forward_resource_stacks:
      :type forward_resource_stacks: list(tuple(ProjectItemResource))
      :param backward_resources:
      :type backward_resources: list(ProjectItemResource)
      :param timestamp: timestamp for the execution filter
      :type timestamp: str

      :returns: forward resources, backward resources, filter id
      :rtype: Iterator(tuple(list,list,str))


   .. py:method:: _expand_resource_stack(self, item_name, resource_stack)

      Expands a resource stack if possible.

      If the stack has more than one resource, returns the unaltered stack.

      Otherwise, if the stack has only one resource but there are no filters defined for that resource,
      again, returns the unaltered stack.

      Otherwise, returns an expanded stack of as many resources as filter stacks defined for the only one resource.
      Each resource in the expanded stack is a clone of the original, with one of the filter stacks
      applied to the URL.

      :param item_name: resource receiving item's name
      :type item_name: str
      :param resource_stack:
      :type resource_stack: tuple(ProjectItemResource)

      :returns: tuple(ProjectItemResource)


   .. py:method:: _filter_stacks(self, item_name, resource_label)

      Computes filter stacks.

      Stacks are computed as the cross-product of all individual filters defined for a resource.

      :param item_name: item's name
      :type item_name: str
      :param resource_label: resource's label
      :type resource_label: str

      :returns: filter stacks
      :rtype: list of list


   .. py:method:: _convert_forward_resources(self, item_name, resources)

      Converts resources as they're being forwarded to given item.
      The conversion is dictated by the connection the resources traverse in order to reach the item.

      :param item_name: receiving item's name
      :type item_name: str
      :param resources: resources to convert
      :type resources: list of ProjectItemResource

      :returns: converted resources
      :rtype: list of ProjectItemResource


   .. py:method:: _make_dependencies(self)

      Returns a dictionary of dependencies according to the given dictionaries of injectors.

      :returns: a dictionary to pass to the PipelineDefinition constructor as dependencies
      :rtype: dict



.. py:class:: SpineEngineState

   Bases: :py:obj:`enum.Enum`

   Generic enumeration.

   Derive from this class to define new enumerations.

   .. py:attribute:: SLEEPING
      :annotation: = 1

      

   .. py:attribute:: RUNNING
      :annotation: = 2

      

   .. py:attribute:: USER_STOPPED
      :annotation: = 3

      

   .. py:attribute:: FAILED
      :annotation: = 4

      

   .. py:attribute:: COMPLETED
      :annotation: = 5

      

   .. py:method:: __str__(self)

      Return str(self).



.. py:class:: ItemExecutionFinishState

   Bases: :py:obj:`enum.Enum`

   Generic enumeration.

   Derive from this class to define new enumerations.

   .. py:attribute:: SUCCESS
      :annotation: = 1

      

   .. py:attribute:: FAILURE
      :annotation: = 2

      

   .. py:attribute:: SKIPPED
      :annotation: = 3

      

   .. py:attribute:: EXCLUDED
      :annotation: = 4

      

   .. py:attribute:: STOPPED
      :annotation: = 5

      

   .. py:attribute:: NEVER_FINISHED
      :annotation: = 6

      

   .. py:method:: __str__(self)

      Return str(self).



.. py:data:: __version__
   :annotation: = 0.10.1

   

