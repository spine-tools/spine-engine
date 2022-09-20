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
Provides connection classes for linking project items.

:authors: A. Soininen (VTT)
:date:    12.2.2021
"""
import os
import subprocess
import tempfile
from contextlib import ExitStack
from datapackage import Package
from spinedb_api import DatabaseMapping, SpineDBAPIError, SpineDBVersionError
from spinedb_api.filters.scenario_filter import SCENARIO_FILTER_TYPE
from spinedb_api.filters.tool_filter import TOOL_FILTER_TYPE
from spine_engine.project_item.project_item_resource import (
    file_resource,
    cmd_line_arg_from_dict,
    expand_cmd_line_args,
    labelled_resource_args,
)
from spine_engine.utils.helpers import resolve_python_interpreter
from spine_engine.utils.queue_logger import QueueLogger


class ConnectionBase:
    """Base class for connections between two project items."""

    def __init__(self, source_name, source_position, destination_name, destination_position):
        """
        Args:
            source_name (str): source project item's name
            source_position (str): source anchor's position
            destination_name (str): destination project item's name
            destination_position (str): destination anchor's position
        """
        self.source = source_name
        self._source_position = source_position
        self.destination = destination_name
        self._destination_position = destination_position
        self._logger = None

    def __eq__(self, other):
        if not isinstance(other, ConnectionBase):
            return NotImplemented
        return (
            self.source == other.source
            and self._source_position == other._source_position
            and self.destination == other.destination
            and self._destination_position == other._destination_position
        )

    @property
    def name(self):
        return f"from {self.source} to {self.destination}"

    @property
    def destination_position(self):
        """Anchor's position on destination item."""
        return self._destination_position

    @destination_position.setter
    def destination_position(self, destination_position):
        """Set anchor's position on destination item."""
        self._destination_position = destination_position

    @property
    def source_position(self):
        """Anchor's position on source item."""
        return self._source_position

    @source_position.setter
    def source_position(self, source_position):
        """Set anchor's position on source item."""
        self._source_position = source_position

    def __hash__(self):
        return hash((self.source, self._source_position, self.destination, self._destination_position))

    def to_dict(self):
        """Returns a dictionary representation of this connection.

        Returns:
            dict: serialized Connection
        """
        return {
            "name": self.name,
            "from": [self.source, self._source_position],
            "to": [self.destination, self._destination_position],
        }

    @staticmethod
    def _constructor_args_from_dict(connection_dict):
        """Parses __init__() arguments from serialized connection.

        Args:
            connection_dict (dict): serialized ConnectionBase

        Returns:
            dict: keyword arguments suitable for constructing ConnectionBase
        """
        source_name, source_anchor = connection_dict["from"]
        destination_name, destination_anchor = connection_dict["to"]
        return {
            "source_name": source_name,
            "source_position": source_anchor,
            "destination_name": destination_name,
            "destination_position": destination_anchor,
        }

    def receive_resources_from_source(self, resources):
        """
        Receives resources from source item.

        Args:
            resources (Iterable of ProjectItemResource): source item's resources
        """

    def receive_resources_from_destination(self, resources):
        """
        Receives resources from destination item.

        Args:
            resources (Iterable of ProjectItemResource): destination item's resources
        """

    def make_logger(self, queue):
        self._logger = QueueLogger(queue, self.name, None, dict())

    def emit_flash(self):
        self._logger.flash.emit()


class ResourceConvertingConnection(ConnectionBase):
    def __init__(
        self,
        source_name,
        source_position,
        destination_name,
        destination_position,
        options=None,
        disabled_filter_names=None,
    ):
        """
        Args:
            source_name (str): source project item's name
            source_position (str): source anchor's position
            destination_name (str): destination project item's name
            destination_position (str): destination anchor's position
            options (dict, optional): any additional options
            disabled_filter_names (dict, optional): mapping from resource labels and filter types
                to offline filter names
        """
        super().__init__(source_name, source_position, destination_name, destination_position)
        self._disabled_filter_names = disabled_filter_names if disabled_filter_names is not None else {}
        self.options = options if options is not None else dict()
        self._resources = set()

    def __eq__(self, other):
        if not isinstance(other, ResourceConvertingConnection):
            return NotImplemented
        return (
            super().__eq__(other)
            and self._disabled_filter_names == other._disabled_filter_names
            and self.options == other.options
        )

    @property
    def use_datapackage(self):
        """True if datapackage is used, False otherwise"""
        return self.options.get("use_datapackage", False)

    @property
    def use_memory_db(self):
        """True if in-memory database is used, False otherwise"""
        return self.options.get("use_memory_db", False)

    @property
    def write_index(self):
        """The index this connection has in concurrent writing. Defaults to 1, lower writes earlier.
        If two or more connections have the same, then no order is enforced among them.
        """
        return self.options.get("write_index", 1)

    def _has_disabled_filters(self):
        """Return True if connection has disabled filters.

        Returns:
            bool: True if connection has disabled filters, False otherwise
        """
        for names_by_filter_type in self._disabled_filter_names.values():
            for names in names_by_filter_type.values():
                if names:
                    return True
        return False

    def receive_resources_from_source(self, resources):
        """See base class."""
        self._resources = {r for r in resources if r.type_ == "database" and r.filterable}

    def convert_backward_resources(self, resources, sibling_connections):
        """Called when advertising resources through this connection *in the BACKWARD direction*.
        Takes the initial list of resources advertised by the destination item and returns a new list,
        which is the one finally advertised.

        Args:
            resources (list of ProjectItemResource): Resources to convert
            sibling_connections (list of Connection): Sibling connections

        Returns:
            list of ProjectItemResource
        """
        return self._apply_use_memory_db(self._apply_write_index(resources, sibling_connections))

    def convert_forward_resources(self, resources):
        """Called when advertising resources through this connection *in the FORWARD direction*.
        Takes the initial list of resources advertised by the source item and returns a new list,
        which is the one finally advertised.

        Args:
            resources (list of ProjectItemResource): Resources to convert

        Returns:
            list of ProjectItemResource
        """
        return self._apply_use_memory_db(self._apply_use_datapackage(resources))

    def _apply_use_memory_db(self, resources):
        if not self.use_memory_db:
            return resources
        final_resources = []
        for r in resources:
            if r.type_ == "database":
                r = r.clone(additional_metadata={"memory": True})
            final_resources.append(r)
        return final_resources

    def _apply_write_index(self, resources, sibling_connections):
        final_resources = []
        precursors = [c.name for c in sibling_connections if c.write_index < self.write_index]
        for r in resources:
            if r.type_ == "database":
                r = r.clone(additional_metadata={"consumer": self.name, "precursors": precursors})
            final_resources.append(r)
        return final_resources

    def _apply_use_datapackage(self, resources):
        if not self.use_datapackage:
            return resources
        # Split CSVs from the rest of resources
        final_resources = []
        csv_filepaths = []
        for r in resources:
            if r.hasfilepath and os.path.splitext(r.path)[1].lower() == ".csv":
                csv_filepaths.append(r.path)
                continue
            final_resources.append(r)
        if not csv_filepaths:
            return final_resources
        # Build Package from CSVs and add it to the resources
        base_path = os.path.dirname(os.path.commonpath(csv_filepaths))
        package = Package(base_path=base_path)
        for path in csv_filepaths:
            package.add_resource({"path": os.path.relpath(path, base_path)})
        package_path = os.path.join(base_path, "datapackage.json")
        package.save(package_path)
        provider = resources[0].provider_name
        package_resource = file_resource(provider, package_path, label=f"datapackage@{provider}")
        package_resource.metadata = resources[0].metadata
        final_resources.append(package_resource)
        return final_resources

    def to_dict(self):
        """Returns a dictionary representation of this Connection.

        Returns:
            dict: serialized Connection
        """
        d = super().to_dict()
        if self.options:
            d["options"] = self.options.copy()
        if self._has_disabled_filters():
            d["disabled_filters"] = _sets_to_lists(self._disabled_filter_names)
        return d

    @staticmethod
    def _constructor_args_from_dict(connection_dict):
        """See base class."""
        kw_args = ConnectionBase._constructor_args_from_dict(connection_dict)
        kw_args["options"] = connection_dict.get("options")
        disabled_names = connection_dict.get("disabled_filters")
        kw_args["disabled_filter_names"] = _lists_to_sets(disabled_names) if disabled_names is not None else None
        return kw_args


class Connection(ResourceConvertingConnection):
    """Represents a connection between two project items."""

    def __init__(
        self,
        source_name,
        source_position,
        destination_name,
        destination_position,
        options=None,
        disabled_filter_names=None,
    ):
        """
        Args:
            source_name (str): source project item's name
            source_position (str): source anchor's position
            destination_name (str): destination project item's name
            destination_position (str): destination anchor's position
            options (dict, optional): any additional options
            disabled_filter_names (dict, optional): mapping from resource labels and filter types
                to offline filter names
        """
        super().__init__(
            source_name, source_position, destination_name, destination_position, options, disabled_filter_names
        )
        self._enabled_filter_names = None
        self._source_visited = False

    def visit_source(self):
        self._source_visited = True

    def visit_destination(self):
        if not self._source_visited:
            # Can happen in loop execution
            return
        self._source_visited = False
        self.emit_flash()

    def enabled_filters(self, resource_label):
        """Returns enabled filter names for given resource label.

        Args:
            resource_label (str): resource label

        Returns:
            dict: mapping from filter type to list of online filter names
        """
        if self._enabled_filter_names is None:
            self._prepare_enabled_filter_names()
        return self._enabled_filter_names.get(resource_label)

    def _prepare_enabled_filter_names(self):
        """Reads filter information from database."""
        self._enabled_filter_names = {}
        for resource in self._resources:
            url = resource.url
            if not url:
                continue
            try:
                db_map = DatabaseMapping(url)
            except (SpineDBAPIError, SpineDBVersionError):
                continue
            try:
                disabled_scenarios = self._disabled_filter_names.get(resource.label, {}).get(
                    SCENARIO_FILTER_TYPE, set()
                )
                available_scenarios = {row.name for row in db_map.query(db_map.scenario_sq)}
                enabled_scenarios = available_scenarios - disabled_scenarios
                if enabled_scenarios:
                    self._enabled_filter_names.setdefault(resource.label, {})[SCENARIO_FILTER_TYPE] = sorted(
                        list(enabled_scenarios)
                    )
                disabled_tools = set(self._disabled_filter_names.get(resource.label, {}).get(TOOL_FILTER_TYPE, set()))
                available_tools = {row.name for row in db_map.query(db_map.tool_sq)}
                enabled_tools = available_tools - disabled_tools
                if enabled_tools:
                    self._enabled_filter_names.setdefault(resource.label, {})[TOOL_FILTER_TYPE] = sorted(
                        list(enabled_tools)
                    )
            finally:
                db_map.connection.close()

    @classmethod
    def from_dict(cls, connection_dict):
        """Restores a connection from dictionary.

        Args:
            connection_dict (dict): connection dictionary

        Returns:
            Connection: restored connection
        """
        kw_args_from_dict = cls._constructor_args_from_dict(connection_dict)
        return cls(**kw_args_from_dict)


class Jump(ConnectionBase):
    """Represents a conditional jump between two project items."""

    def __init__(
        self,
        source_name,
        source_position,
        destination_name,
        destination_position,
        condition="exit(1)",
        cmd_line_args=(),
    ):
        """
        Args:
            source_name (str): source project item's name
            source_position (str): source anchor's position
            destination_name (str): destination project item's name
            destination_position (str): destination anchor's position
            condition (str): jump condition
        """
        super().__init__(source_name, source_position, destination_name, destination_position)
        self.condition = condition
        self._resources_from_source = set()
        self._resources_from_destination = set()
        self.cmd_line_args = list(cmd_line_args)

    @property
    def resources(self):
        return self._resources_from_source | self._resources_from_destination

    def update_cmd_line_args(self, cmd_line_args):
        self.cmd_line_args = cmd_line_args

    def receive_resources_from_source(self, resources):
        """See base class."""
        self._resources_from_source = set(resources)

    def receive_resources_from_destination(self, resources):
        """See base class."""
        self._resources_from_destination = set(resources)

    def is_condition_true(self, jump_counter):
        """Evaluates jump condition.

        Args:
            jump_counter (int): how many times jump has been executed

        Returns:
            bool: True if jump should be executed, False otherwise
        """
        if not self.condition.strip():
            return False
        with ExitStack() as stack:
            labelled_args = labelled_resource_args(self.resources, stack)
            expanded_args = expand_cmd_line_args(self.cmd_line_args, labelled_args, self._logger)
            expanded_args.append(str(jump_counter))
            with tempfile.TemporaryFile("w+", encoding="utf-8") as script:
                script.write(self.condition)
                script.seek(0)
                python = resolve_python_interpreter("")
                result = subprocess.run(
                    [python, "-", *expanded_args], encoding="utf-8", stdin=script, capture_output=True
                )
                if result.stdout:
                    self._logger.msg_proc.emit(result.stdout)
                if result.stderr:
                    self._logger.msg_proc_error.emit(result.stderr)
                iterate = result.returncode == 0
                if iterate:
                    self.emit_flash()
                return iterate

    @classmethod
    def from_dict(cls, jump_dict, **kwargs):
        """Restores a Jump from dictionary.

        Args:
            jump_dict (dict): serialized jump
            **kwargs: extra keyword arguments passed to constructor

        Returns:
            Jump: restored jump
        """
        super_kw_ags = cls._constructor_args_from_dict(jump_dict)
        condition = jump_dict["condition"]["script"]
        cmd_line_args = jump_dict.get("cmd_line_args", [])
        cmd_line_args = [cmd_line_arg_from_dict(arg) for arg in cmd_line_args]
        return cls(condition=condition, cmd_line_args=cmd_line_args, **super_kw_ags, **kwargs)

    def to_dict(self):
        """Returns a dictionary representation of this Jump.

        Returns:
            dict: serialized Jump
        """
        d = super().to_dict()
        d["condition"] = {"type": "python-script", "script": self.condition}
        d["cmd_line_args"] = [arg.to_dict() for arg in self.cmd_line_args]
        return d


def _sets_to_lists(disabled_filter_names):
    """Converts name sets to sorted name lists.

    Args:
        disabled_filter_names (dict): connection's disabled filter names

    Returns:
        dict: converted disabled filter names
    """
    converted = {}
    for label, names_by_type in disabled_filter_names.items():
        converted_names_by_type = converted.setdefault(label, {})
        for filter_type, names in names_by_type.items():
            converted_names_by_type[filter_type] = sorted(list(names))
    return converted


def _lists_to_sets(disabled_filter_names):
    """Converts name lists to name sets.

    Args:
        disabled_filter_names (dict): disabled filter names with names stored as lists

    Returns:
        dict: converted disabled filter names
    """
    converted = {}
    for label, names_by_type in disabled_filter_names.items():
        converted_names_by_type = converted.setdefault(label, {})
        for filter_type, names in names_by_type.items():
            converted_names_by_type[filter_type] = set(names)
    return converted
