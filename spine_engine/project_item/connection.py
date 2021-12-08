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

    @property
    def source_position(self):
        """Anchor's position on source item."""
        return self._source_position

    def to_dict(self):
        """Returns a dictionary representation of this connection.

        Returns:
            dict: serialized Connection
        """
        return {"from": [self.source, self._source_position], "to": [self.destination, self._destination_position]}

    def receive_resources_from_source(self, resources):
        pass

    def receive_resources_from_destination(self, resources):
        pass


class Connection(ConnectionBase):
    """Represents a connection between two project items."""

    def __init__(
        self, source_name, source_position, destination_name, destination_position, resource_filters=None, options=None
    ):
        """
        Args:
            source_name (str): source project item's name
            source_position (str): source anchor's position
            destination_name (str): destination project item's name
            destination_position (str): destination anchor's position
            resource_filters (dict, optional): mapping from resource labels and filter types to
                database ids and activity flags
            options (dict, optional): any options, at the moment only has "use_datapackage"
        """
        super().__init__(source_name, source_position, destination_name, destination_position)
        self._resource_filters = resource_filters if resource_filters is not None else dict()
        self.options = options if options is not None else dict()
        self._resources = set()
        self._id_to_name_cache = dict()

    def __eq__(self, other):
        if not isinstance(other, Connection):
            return NotImplemented
        return (
            super().__eq__(other)
            and self._resource_filters == other._resource_filters
            and self.options == other.options
        )

    @property
    def database_resources(self):
        """Connection's database resources"""
        return self._resources

    def has_filters(self):
        """Return True if connection has filters.

        Returns:
            bool: True if connection has filters, False otherwise
        """
        for ids_by_type in self._resource_filters.values():
            for ids in ids_by_type.values():
                if any(ids.values()):
                    return True
        return False

    @property
    def resource_filters(self):
        """Connection's resource filters."""
        return self._resource_filters

    @property
    def use_datapackage(self):
        return self.options.get("use_datapackage", False)

    def id_to_name(self, id_, filter_type):
        """Map from scenario/tool database id to name"""
        return self._id_to_name_cache[filter_type][id_]

    def receive_resources_from_source(self, resources):
        """
        Receives resources from source item.

        Args:
            resources (Iterable of ProjectItemResource): source item's resources
        """
        self._resources = {r for r in resources if r.type_ == "database"}

    def replace_resources_from_source(self, old, new):
        """Replaces existing resources by new ones.

        Args:
            old (list of ProjectItemResource): old resources
            new (list of ProjectItemResource): new resources
        """
        for old_resource, new_resource in zip(old, new):
            self._resources.discard(old_resource)
            old_filters = self._resource_filters.pop(old_resource.label, None)
            if new_resource.type_ == "database":
                self._resources.add(new_resource)
                if old_filters is not None:
                    self._resource_filters[new_resource.label] = old_filters

    def fetch_database_items(self):
        """Reads filter information from database."""
        resource_filters = dict()
        id_to_name_cache = dict()

        def update_filters(label, filter_type, db_row):
            filters_by_type = self._resource_filters.get(label)
            is_on = False
            if filters_by_type is not None:
                ids = filters_by_type.get(filter_type)
                if ids is not None:
                    currently_on = ids.get(db_row.id)
                    if currently_on is not None:
                        is_on = currently_on
            resource_filters.setdefault(label, dict()).setdefault(filter_type, dict())[db_row.id] = is_on
            id_to_name_cache.setdefault(filter_type, dict())[db_row.id] = db_row.name

        for resource in self._resources:
            url = resource.url
            if not url:
                continue
            try:
                db_map = DatabaseMapping(url)
            except (SpineDBAPIError, SpineDBVersionError):
                continue
            try:
                for scenario_row in db_map.query(db_map.scenario_sq):
                    update_filters(resource.label, SCENARIO_FILTER_TYPE, scenario_row)
                for tool_row in db_map.query(db_map.tool_sq):
                    update_filters(resource.label, TOOL_FILTER_TYPE, tool_row)
            finally:
                db_map.connection.close()
        self._resource_filters = resource_filters
        self._id_to_name_cache = id_to_name_cache

    def set_online(self, resource, filter_type, online):
        """Sets the given filters online or offline.

        Args:
            resource (str): Resource label
            filter_type (str): Either SCENARIO_FILTER_TYPE or TOOL_FILTER_TYPE, for now.
            online (dict): mapping from scenario/tool id to online flag
        """
        current_ids = self._resource_filters[resource][filter_type]
        for id_, online_ in online.items():
            current_ids[id_] = online_

    def convert_resources(self, resources, override_provider_name=None):
        """Called when advertising resources through this connection *in the FORWARD direction*.
        Takes the initial list of resources advertised by the source item and returns a new list,
        which is the one finally advertised.

        At the moment it only packs CSVs into datapackage (and again, it's only used in the FORWARD direction).

        Args:
            resources (list of ProjectItemResource): Resources to convert
            override_provider_name (str, optional): set converted resources provider name to this; use source if None

        Returns:
            list of ProjectItemResource
        """
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
        provider = self.source if override_provider_name is None else override_provider_name
        package_resource = file_resource(provider, package_path, label=f"datapackage@{provider}")
        final_resources.append(package_resource)
        return final_resources

    def to_dict(self):
        """Returns a dictionary representation of this Connection.

        Returns:
            dict: serialized Connection
        """
        d = super().to_dict()
        if self.has_filters():
            online_ids_only = dict()
            for label, by_type in self._resource_filters.items():
                for type_, ids in by_type.items():
                    online = [id_ for id_, is_on in ids.items() if is_on]
                    if online:
                        online_ids_only.setdefault(label, {})[type_] = online
            d["resource_filters"] = online_ids_only
        if self.options:
            d["options"] = self.options.copy()
        return d

    @staticmethod
    def from_dict(connection_dict):
        """Restores a connection from dictionary.

        Args:
            connection_dict (dict): connection dictionary

        Returns:
            Connection: restored connection
        """
        source_name, source_anchor = connection_dict["from"]
        destination_name, destination_anchor = connection_dict["to"]
        resource_filters_dict = connection_dict.get("resource_filters")
        resource_filters = dict()
        if resource_filters_dict is not None:
            for label, filters_by_type in resource_filters_dict.items():
                for type_, ids in filters_by_type.items():
                    for id_ in ids:
                        resource_filters.setdefault(label, {}).setdefault(type_, {})[id_] = True
        options = connection_dict.get("options")
        return Connection(source_name, source_anchor, destination_name, destination_anchor, resource_filters, options)


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
        self.resources = set()
        self.cmd_line_args = list(cmd_line_args)

    def update_cmd_line_args(self, cmd_line_args):
        self.cmd_line_args = cmd_line_args

    def receive_resources_from_source(self, resources):
        self.resources.update(resources)

    def receive_resources_from_destination(self, resources):
        self.resources.update(resources)

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
            expanded_args = expand_cmd_line_args(self.cmd_line_args, labelled_args, None)
            expanded_args.append(str(jump_counter))
            with tempfile.TemporaryFile("w+", encoding="utf-8") as script:
                script.write(self.condition)
                script.seek(0)
                result = subprocess.run(
                    ["python", "-", *expanded_args], encoding="utf-8", stdin=script, capture_output=True
                )
                return result.returncode == 0

    @staticmethod
    def from_dict(jump_dict):
        """Restores a Jump from dictionary.

        Args:
            jump_dict (dict): serialized jump

        Returns:
            Jump: restored jump
        """
        source_name, source_anchor = jump_dict["from"]
        destination_name, destination_anchor = jump_dict["to"]
        condition = jump_dict["condition"]["script"]
        cmd_line_args = jump_dict.get("cmd_line_args", [])
        cmd_line_args = [cmd_line_arg_from_dict(arg) for arg in cmd_line_args]
        return Jump(source_name, source_anchor, destination_name, destination_anchor, condition, cmd_line_args)

    def to_dict(self):
        """Returns a dictionary representation of this Jump.

        Returns:
            dict: serialized Jump
        """
        d = super().to_dict()
        d["condition"] = {"type": "python-script", "script": self.condition}
        d["cmd_line_args"] = [arg.to_dict() for arg in self.cmd_line_args]
        return d
