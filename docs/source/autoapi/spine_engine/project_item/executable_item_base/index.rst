:py:mod:`spine_engine.project_item.executable_item_base`
========================================================

.. py:module:: spine_engine.project_item.executable_item_base

.. autoapi-nested-parse::

   Contains ExecutableItem, a project item's counterpart in execution as well as support utilities.

   :authors: A. Soininen (VTT)
   :date:    30.3.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.project_item.executable_item_base.ExecutableItemBase




.. py:class:: ExecutableItemBase(name, project_dir, logger)

   The part of a project item that is executed by the Spine Engine.

   :param name: item's name
   :type name: str
   :param project_dir: absolute path to project directory
   :type project_dir: str
   :param logger: a logger
   :type logger: LoggerInterface

   .. py:method:: name(self)
      :property:

      Project item's name.


   .. py:method:: group_id(self)
      :property:

      Returns the id for group-execution.
      Items in the same group share a kernel, and also reuse the same kernel from past executions.
      By default each item is its own group, so it executes in isolation.
      NOTE: At the moment this is only used by Tool, but could be used by other items in the future?

      :returns: item's id within an execution group
      :rtype: str


   .. py:method:: filter_id(self)
      :property:


   .. py:method:: ready_to_execute(self, settings)

      Validates the internal state of this project item before execution.

      Subclasses can implement this method to do the appropriate work.

      :param settings: Application settings
      :type settings: AppSettings

      :returns: True if project item is ready for execution, False otherwise
      :rtype: bool


   .. py:method:: execute(self, forward_resources, backward_resources)

      Executes this item using the given resources and returns a boolean indicating the outcome.

      Subclasses can implement this method to do the appropriate work.

      :param forward_resources: a list of ProjectItemResources from predecessors (forward)
      :type forward_resources: list
      :param backward_resources: a list of ProjectItemResources from successors (backward)
      :type backward_resources: list

      :returns: State depending on operation success
      :rtype: ItemExecutionFinishState


   .. py:method:: exclude_execution(self, forward_resources, backward_resources)

      Excludes execution of this item.

      This method is called when the item is not selected (i.e EXCLUDED) for execution.
      Only lightweight bookkeeping or processing should be done in this case, e.g.
      forward input resources.

      Subclasses can implement this method to the appropriate work.

      :param forward_resources: a list of ProjectItemResources from predecessors (forward)
      :type forward_resources: list
      :param backward_resources: a list of ProjectItemResources from successors (backward)
      :type backward_resources: list


   .. py:method:: finish_execution(self, state)

      Does any work needed after execution given the execution success status.

      :param state: Item execution finish state
      :type state: ItemExecutionFinishState


   .. py:method:: item_type()
      :staticmethod:
      :abstractmethod:

      Returns the item's type identifier string.


   .. py:method:: output_resources(self, direction)

      Returns output resources in the given direction.

      Subclasses need to implement _output_resources_backward and/or _output_resources_forward
      if they want to provide resources in any direction.

      :param direction: Direction where output resources are passed
      :type direction: ExecutionDirection

      :returns: a list of ProjectItemResources
      :rtype: list


   .. py:method:: stop_execution(self)

      Stops executing this item.


   .. py:method:: _output_resources_forward(self)

      Returns output resources for forward execution.

      The default implementation returns an empty list.

      :returns: a list of ProjectItemResources
      :rtype: list


   .. py:method:: _output_resources_backward(self)

      Returns output resources for backward execution.

      The default implementation returns an empty list.

      :returns: a list of ProjectItemResources
      :rtype: list


   .. py:method:: from_dict(cls, item_dict, name, project_dir, app_settings, specifications, logger)
      :classmethod:
      :abstractmethod:

      Deserializes an executable item from item dictionary.

      :param item_dict: serialized project item
      :type item_dict: dict
      :param name: item's name
      :type name: str
      :param project_dir: absolute path to the project directory
      :type project_dir: str
      :param app_settings: Toolbox settings
      :type app_settings: QSettings
      :param specifications: mapping from item type to specification name to :class:`ProjectItemSpecification`
      :type specifications: dict
      :param logger: a logger
      :type logger: LoggingInterface

      :returns: deserialized executable item
      :rtype: ExecutableItemBase


   .. py:method:: _get_specification(name, item_type, specification_name, specifications, logger)
      :staticmethod:



