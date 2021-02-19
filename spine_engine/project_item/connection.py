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
Provides the :class:`Connection` class.

:authors: A. Soininen (VTT)
:date:    12.2.2021
"""
from spinedb_api import DatabaseMapping
from spinedb_api.filters.scenario_filter import SCENARIO_FILTER_TYPE
from spinedb_api.filters.tool_filter import TOOL_FILTER_TYPE


class Connection:
    """Represents a connection between two project items."""

    def __init__(self, source_name, source_position, destination_name, destination_position, resource_filters=None):
        """
        Args:
            source_name (str): source project item's name
            source_position (str): source anchor's position
            destination_name (str): destination project item's name
            destination_position (str): destination anchor's position
            resource_filters (dict, optional): mapping from resource labels and filter types to
                database ids and activity flags
        """
        self.source = source_name
        self._source_position = source_position
        self.destination = destination_name
        self._destination_position = destination_position
        self._resource_filters = resource_filters if resource_filters is not None else dict()
        self._resources = set()
        self._id_to_name_cache = dict()

    def __eq__(self, other):
        if not isinstance(other, Connection):
            return NotImplemented
        return (
            self.source == other.source
            and self._source_position == other._source_position
            and self.destination == other.destination
            and self._destination_position == other._destination_position
            and self._resource_filters == other._resource_filters
        )

    @property
    def destination_position(self):
        """Anchor's position on destination item."""
        return self._destination_position

    @property
    def source_position(self):
        """Anchor's position on source item."""
        return self._source_position

    @property
    def database_resources(self):
        """Connection's database resources"""
        return self._resources

    def has_filters(self):
        """Return True if connection has filters.

        Returns:
            bool: True if connection has filters, False otherwise
        """
        return bool(self._resource_filters)

    @property
    def resource_filters(self):
        """Connection's resource filters."""
        return self._resource_filters

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
                return
            db_map = DatabaseMapping(url)
            try:
                if db_map is None:
                    continue
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
        for id_, online in online.items():
            current_ids[id_] = online

    def to_dict(self):
        """Returns a dictionary representation of this Connection.

        Returns:
            dict: serialized Connection
        """
        d = {"from": [self.source, self._source_position], "to": [self.destination, self._destination_position]}
        if self.has_filters():
            online_ids_only = dict()
            for label, by_type in self._resource_filters.items():
                for type_, ids in by_type.items():
                    online = [id_ for id_, is_on in ids.items() if is_on]
                    if online:
                        online_ids_only.setdefault(label, {})[type_] = online
            d["resource_filters"] = online_ids_only
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
        return Connection(source_name, source_anchor, destination_name, destination_anchor, resource_filters)
