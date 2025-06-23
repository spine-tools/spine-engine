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
from __future__ import annotations
from dataclasses import asdict, dataclass, field
from spinedb_api.filters.alternative_filter import ALTERNATIVE_FILTER_TYPE
from spinedb_api.filters.scenario_filter import SCENARIO_FILTER_TYPE

SUPPORTED_FILTER_TYPES = {ALTERNATIVE_FILTER_TYPE, SCENARIO_FILTER_TYPE}
DEFAULT_ENABLED_FILTER_TYPES = {ALTERNATIVE_FILTER_TYPE: False, SCENARIO_FILTER_TYPE: True}


@dataclass
class FilterSettings:
    """Filter settings for resource converting connections."""

    known_filters: dict[str, dict[str, dict[str, bool]]] = field(default_factory=dict)
    """mapping from resource labels and filter types to filter online statuses"""
    auto_online: bool = True
    """if True, set unknown filters automatically online"""
    enabled_filter_types: dict[str, bool] = field(default_factory=DEFAULT_ENABLED_FILTER_TYPES.copy)

    def __post_init__(self):
        for resource, online_filters in self.known_filters.items():
            supported_filters = {
                filter_type: online
                for filter_type, online in online_filters.items()
                if filter_type in SUPPORTED_FILTER_TYPES
            }
            if supported_filters:
                self.known_filters[resource] = supported_filters

    def has_filters(self) -> bool:
        """Tests if there are filters.

        Returns:
            True if filters of any type exists, False otherwise
        """
        for filters_by_type in self.known_filters.values():
            for filter_type, filters in filters_by_type.items():
                if self.enabled_filter_types[filter_type] and filters:
                    return True
        return False

    def has_any_filter_online(self) -> bool:
        """Tests in any filter is online.

        Returns:
            True if any filter is online, False otherwise
        """
        for filters_by_type in self.known_filters.values():
            for filter_type, filters in filters_by_type.items():
                if self.enabled_filter_types[filter_type] and any(filters.values()):
                    return True
        return False

    def has_filter_online(self, filter_type: str) -> bool:
        """Tests if any filter of given type is online.

        Args:
            filter_type: filter type to test

        Returns:
            True if any filter of filter_type is online, False otherwise
        """
        if not self.enabled_filter_types[filter_type]:
            return False
        for filters_by_type in self.known_filters.values():
            if any(filters_by_type.get(filter_type, {}).values()):
                return True
        return False

    def to_dict(self) -> dict:
        """Stores the settings to a dict.

        Returns:
            serialized settings
        """
        return asdict(self)

    @staticmethod
    def from_dict(settings_dict: dict) -> FilterSettings:
        """Restores the settings from a dict.

        Args:
            settings_dict: serialized settings

        Returns:
            restored settings
        """
        return FilterSettings(**settings_dict)
