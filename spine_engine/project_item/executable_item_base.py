######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains ExecutableItem, a project item's counterpart in execution as well as support utilities.

:authors: A. Soininen (VTT)
:date:   30.3.2020
"""

from spine_engine import ExecutionDirection


class ExecutableItemBase:
    """
    The part of a project item that is executed by the Spine Engine.
    """

    def __init__(self, name, logger):
        """
        Args:
            name (str): item's name
            logger (LoggerInterface): a logger
        """
        self._name = name
        self._logger = logger
        self._filter_id = ""

    @property
    def name(self):
        """Project item's name."""
        return self._name

    @property
    def group_id(self):
        """Returns the id for group-execution.
        Items in the same group share a kernel, and also reuse the same kernel from past executions.
        By default each item is its own group, so it executes in isolation.
        NOTE: At the moment this is only used by Tool, but could be used by other items in the future?
        """
        return self._name + self._filter_id

    @property
    def filter_id(self):
        return self._filter_id

    @filter_id.setter
    def filter_id(self, filter_id):
        self._filter_id = filter_id
        self._logger.set_filter_id(filter_id)

    def execute(self, forward_resources, backward_resources):
        """
        Executes this item using the given resources and returns a boolean indicating the outcome.

        Subclasses can implement this method to do the appropriate work.

        Args:
            forward_resources (list): a list of ProjectItemResources from predecesors (forward)
            backward_resources (list): a list of ProjectItemResources from successors (backward)

        Returns:
            bool: True if execution succeeded, False otherwise
        """
        self._logger.msg.emit(f"***Executing {self.item_type()} <b>{self._name}</b>***")
        return True

    def skip_execution(self, forward_resources, backward_resources):
        """
        Skips executing the item.

        This method is called when the item is not selected for execution. Only lightweight bookkeeping
        or processing should be done in this case, e.g. forward input resources.

        Subclasses can implement this method to the appropriate work.

        Args:
            forward_resources (list): a list of ProjectItemResources from predecesors (forward)
            backward_resources (list): a list of ProjectItemResources from successors (backward)
        """

    def finish_execution(self, success):
        """
        Does any work needed after execution given the execution success status.

        Args:
            success (bool)
        """
        if success:
            self._logger.msg_success.emit(f"Executing {self.item_type()} {self.name} finished")
        else:
            self._logger.msg_error.emit(f"Executing {self.item_type()} {self.name} failed")

    @staticmethod
    def item_type():
        """Returns the item's type identifier string."""
        raise NotImplementedError()

    def output_resources(self, direction):
        """
        Returns output resources in the given direction.

        Subclasses need to implement _output_resources_backward and/or _output_resources_forward
        if they want to provide resources in any direction.

        Args:
            direction (ExecutionDirection): Direction where output resources are passed

        Returns:
            a list of ProjectItemResources
        """
        return {
            ExecutionDirection.BACKWARD: self._output_resources_backward,
            ExecutionDirection.FORWARD: self._output_resources_forward,
        }[direction]()

    def stop_execution(self):
        """Stops executing this item."""
        self._logger.msg.emit(f"Stopping {self._name}")

    # pylint: disable=no-self-use
    def _output_resources_forward(self):
        """
        Returns output resources for forward execution.

        The default implementation returns an empty list.

        Returns:
            a list of ProjectItemResources
        """
        return list()

    # pylint: disable=no-self-use
    def _output_resources_backward(self):
        """
        Returns output resources for backward execution.

        The default implementation returns an empty list.

        Returns:
            a list of ProjectItemResources
        """
        return list()

    @classmethod
    def from_dict(cls, item_dict, name, project_dir, app_settings, specifications, logger):
        """
        Deserializes an executable item from item dictionary.

        Args:
            item_dict (dict): serialized project item
            name (str): item's name
            project_dir (str): absolute path to the project directory
            app_settings (QSettings): Toolbox settings
            specifications (dict): mapping from item type to specification name to :class:`ProjectItemSpecification`
            logger (LoggingInterface): a logger
        Returns:
            ExecutableItemBase: deserialized executable item
        """
        raise NotImplementedError()

    @staticmethod
    def _get_specification(name, item_type, specification_name, specifications, logger):
        if not specification_name:
            logger.msg_error.emit(f"<b>{name}<b>: No specification defined. Unable to execute.")
            return None
        try:
            return specifications[item_type][specification_name]
        except KeyError as missing:
            if missing == item_type:
                logger.msg_error.emit(f"No specifications defined for item type '{item_type}'.")
                return None
            logger.msg_error.emit(f"Cannot find specification '{missing}'.")
            return None
