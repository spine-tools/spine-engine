######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Engine contributors
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

""" Contains ExecutableItem, a project item's counterpart in execution as well as support utilities. """
from hashlib import sha1
from pathlib import Path
from ..utils.helpers import ExecutionDirection, ItemExecutionFinishState, shorten


class ExecutableItemBase:
    """The part of a project item that is executed by the Spine Engine."""

    def __init__(self, name, project_dir, logger, group_id=None):
        """
        Args:
            name (str): item's name
            project_dir (str): absolute path to project directory
            logger (LoggerInterface): a logger
            group_id (str, optional): execution group identifier
        """
        self._name = name
        self._project_dir = project_dir
        data_dir = Path(self._project_dir, ".spinetoolbox", "items", shorten(name))
        data_dir.mkdir(parents=True, exist_ok=True)
        self._data_dir = str(data_dir)
        logs_dir = Path(self._data_dir, "logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        self._logs_dir = str(logs_dir)
        self._logger = logger
        self._group_id = name if group_id is None else group_id
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

        Returns:
            str: item's id within an execution group
        """
        return self._group_id

    @property
    def filter_id(self):
        return self._filter_id

    def hash_filter_id(self):
        """Hashes filter id.

        Returns:
            str: hash
        """
        return sha1(bytes(self._filter_id, "utf8")).hexdigest() if self._filter_id else ""

    @filter_id.setter
    def filter_id(self, filter_id):
        self._filter_id = filter_id
        self._logger.set_filter_id(filter_id)

    @property
    def data_dir(self):
        return self._data_dir

    def ready_to_execute(self, settings):
        """Validates the internal state of this project item before execution.

        Subclasses can implement this method to do the appropriate work.

        Args:
            settings (AppSettings): Application settings

        Returns:
            bool: True if project item is ready for execution, False otherwise
        """
        return True

    def update(self, forward_resources, backward_resources):
        """Executes tasks that should be done before going into a next iteration of the loop."""
        return True

    def execute(self, forward_resources, backward_resources, lock):
        """Executes this item using the given resources and returns a boolean indicating the outcome.

        Subclasses can implement this method to do the appropriate work.

        Args:
            forward_resources (list): a list of ProjectItemResources from predecessors (forward)
            backward_resources (list): a list of ProjectItemResources from successors (backward)
            lock (Lock): shared lock for parallel executions

        Returns:
            ItemExecutionFinishState: State depending on operation success
        """
        self._logger.msg.emit(f"***Executing {self.item_type()} <b>{self._name}</b>***")
        return ItemExecutionFinishState.SUCCESS

    def exclude_execution(self, forward_resources, backward_resources, lock):
        """Excludes execution of this item.

        This method is called when the item is not selected (i.e EXCLUDED) for execution.
        Only lightweight bookkeeping or processing should be done in this case, e.g.
        forward input resources.

        Subclasses can implement this method to do the appropriate work.

        Args:
            forward_resources (list): a list of ProjectItemResources from predecessors (forward)
            backward_resources (list): a list of ProjectItemResources from successors (backward)
            lock (Lock): shared lock for parallel executions
        """

    def finish_execution(self, state):
        """Does any work needed after execution given the execution success status.

        Args:
            state (ItemExecutionFinishState): Item execution finish state
        """
        if state == ItemExecutionFinishState.SUCCESS:
            self._logger.msg_success.emit(f"Executing {self.item_type()} {self.name} finished")
        elif state == ItemExecutionFinishState.FAILURE:
            self._logger.msg_error.emit(f"Executing {self.item_type()} {self.name} failed")
        elif state == ItemExecutionFinishState.SKIPPED:
            self._logger.msg_warning.emit(f"Executing {self.name} skipped")
        elif state == ItemExecutionFinishState.STOPPED:
            self._logger.msg_error.emit(f"Executing {self.name} stopped")
        else:
            self._logger.msg_error.emit(f"<b>{self.name}</b> finished execution in an unknown state")

    @staticmethod
    def item_type():
        """Returns the item's type identifier string."""
        raise NotImplementedError()

    @staticmethod
    def is_filter_terminus():
        """Tests if the item 'terminates' a forked execution.

        Returns:
            bool: True if forked executions should be joined before the item, False otherwise
        """
        return False

    def output_resources(self, direction):
        """Returns output resources in the given direction.

        Subclasses need to implement _output_resources_backward and/or _output_resources_forward
        if they want to provide resources in any direction.

        Args:
            direction (ExecutionDirection): Direction where output resources are passed

        Returns:
            list: a list of ProjectItemResources
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
        """Returns output resources for forward execution.

        The default implementation returns an empty list.

        Returns:
            list: a list of ProjectItemResources
        """
        return list()

    # pylint: disable=no-self-use
    def _output_resources_backward(self):
        """Returns output resources for backward execution.

        The default implementation returns an empty list.

        Returns:
            list: a list of ProjectItemResources
        """
        return list()

    @classmethod
    def from_dict(cls, item_dict, name, project_dir, app_settings, specifications, logger):
        """Deserializes an executable item from item dictionary.

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
            return None
        try:
            return specifications[item_type][specification_name]
        except KeyError as missing:
            if missing == item_type:
                logger.msg_error.emit(f"No specifications defined for item type '{item_type}'.")
                return None
            logger.msg_error.emit(f"Cannot find specification '{missing}'.")
            return None
