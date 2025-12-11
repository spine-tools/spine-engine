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
"""Provides connection classes for linking project items."""
from __future__ import annotations
from collections.abc import Iterable
from contextlib import ExitStack
from dataclasses import asdict, dataclass, field
from multiprocessing import Lock
import os
from queue import Queue
import subprocess
import tempfile
from typing import TYPE_CHECKING, Any, Final, Literal, TypedDict
from frictionless import Package, Resource
from typing_extensions import NotRequired
from spinedb_api import DatabaseMapping, SpineDBAPIError, SpineDBVersionError
from spinedb_api.filters.alternative_filter import ALTERNATIVE_FILTER_TYPE
from spinedb_api.filters.scenario_filter import SCENARIO_FILTER_TYPE
from spinedb_api.purge import purge_url
from ..utils.helpers import ExecutionDirection as ED
from ..utils.helpers import ItemExecutionFinishState, PartCount, resolve_current_python_interpreter
from ..utils.queue_logger import QueueLogger
from .project_item_resource import (
    CmdLineArg,
    ProjectItemResource,
    expand_cmd_line_args,
    file_resource,
    labelled_resource_args,
    make_cmd_line_arg,
)

if TYPE_CHECKING:
    from .. import SpineEngine


SUPPORTED_FILTER_TYPES: set[str] = {ALTERNATIVE_FILTER_TYPE, SCENARIO_FILTER_TYPE}

ConnectorPosition = Literal["top", "bottom", "left", "right"]


class ConnectionBase:
    """Base class for connections between two project items."""

    def __init__(
        self,
        source_name: str,
        source_position: ConnectorPosition,
        destination_name: str,
        destination_position: ConnectorPosition,
    ):
        """
        Args:
            source_name: source project item's name
            source_position: source anchor's position
            destination_name: destination project item's name
            destination_position: destination anchor's position
        """
        self.source = source_name
        self._source_position = source_position
        self.destination = destination_name
        self._destination_position = destination_position
        self._logger: QueueLogger | None = None

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
    def name(self) -> str:
        return f"from {self.source} to {self.destination}"

    @property
    def destination_position(self) -> ConnectorPosition:
        """Anchor's position on destination item."""
        return self._destination_position

    @destination_position.setter
    def destination_position(self, destination_position: ConnectorPosition) -> None:
        """Set anchor's position on destination item."""
        self._destination_position = destination_position

    @property
    def source_position(self) -> ConnectorPosition:
        """Anchor's position on source item."""
        return self._source_position

    @source_position.setter
    def source_position(self, source_position: ConnectorPosition) -> None:
        """Set anchor's position on source item."""
        self._source_position = source_position

    def __hash__(self):
        return hash((self.source, self._source_position, self.destination, self._destination_position))

    def ready_to_execute(self) -> bool:
        """Validates the internal state of connection before execution.

        Subclasses can implement this method to do the appropriate work.

        Returns:
            True if connection is ready for execution, False otherwise
        """
        return True

    def notifications(self) -> list[str]:
        """Returns connection validation messages.

        Returns:
            notifications
        """
        return []

    def to_dict(self) -> dict:
        """Returns a dictionary representation of this connection.

        Returns:
            serialized Connection
        """
        return {
            "name": self.name,
            "from": [self.source, self._source_position],
            "to": [self.destination, self._destination_position],
        }

    @staticmethod
    def _constructor_args_from_dict(connection_dict: dict) -> dict[str, Any]:
        """Parses __init__() arguments from serialized connection.

        Args:
            connection_dict: serialized ConnectionBase

        Returns:
            keyword arguments suitable for constructing ConnectionBase
        """
        source_name, source_anchor = connection_dict["from"]
        destination_name, destination_anchor = connection_dict["to"]
        return {
            "source_name": source_name,
            "source_position": source_anchor,
            "destination_name": destination_name,
            "destination_position": destination_anchor,
        }

    def receive_resources_from_source(self, resources: Iterable[ProjectItemResource]) -> None:
        """
        Receives resources from source item.

        Args:
            resources: source item's resources
        """

    def receive_resources_from_destination(self, resources: Iterable[ProjectItemResource]) -> None:
        """
        Receives resources from destination item.

        Args:
            resources: destination item's resources
        """

    def make_logger(self, queue: Queue) -> None:
        self._logger = QueueLogger(queue, self.name, None, None)

    def emit_flash(self) -> None:
        self._logger.flash.emit()


DEFAULT_ENABLED_FILTER_TYPES: dict[str, bool] = {ALTERNATIVE_FILTER_TYPE: False, SCENARIO_FILTER_TYPE: True}


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
    def settings_dict_local_entries() -> list[tuple[str, ...]]:
        return [("known_filters",)]

    @staticmethod
    def from_dict(settings_dict: dict) -> FilterSettings:
        """Restores the settings from a dict.

        Args:
            settings_dict: serialized settings

        Returns:
            restored settings
        """
        return FilterSettings(**settings_dict)


class ConnectionOptions(TypedDict):
    use_datapackage: NotRequired[bool]
    use_memory_db: NotRequired[bool]
    purge_before_writing: NotRequired[bool]
    purge_settings: NotRequired[dict[str, bool]]
    write_index: NotRequired[int]


class ResourceConvertingConnection(ConnectionBase):
    def __init__(
        self,
        source_name: str,
        source_position: ConnectorPosition,
        destination_name: str,
        destination_position: ConnectorPosition,
        options: ConnectionOptions | None = None,
        filter_settings: FilterSettings | None = None,
    ):
        """
        Args:
            source_name: source project item's name
            source_position: source anchor's position
            destination_name: destination project item's name
            destination_position: destination anchor's position
            options: any additional options
            filter_settings: filter settings
        """
        super().__init__(source_name, source_position, destination_name, destination_position)
        self._filter_settings = filter_settings if filter_settings is not None else FilterSettings()
        self.options = options if options is not None else dict()
        self._resources: set[ProjectItemResource] = set()

    def __eq__(self, other):
        if not isinstance(other, ResourceConvertingConnection):
            return NotImplemented
        return (
            super().__eq__(other) and self._filter_settings == other._filter_settings and self.options == other.options
        )

    @property
    def use_datapackage(self) -> bool:
        """True if datapackage is used, False otherwise"""
        return self.options.get("use_datapackage", False)

    @property
    def use_memory_db(self) -> bool:
        """True if in-memory database is used, False otherwise"""
        return self.options.get("use_memory_db", False)

    @property
    def purge_before_writing(self) -> bool:
        """True if purge before writing is active, False otherwise"""
        return self.options.get("purge_before_writing", False)

    @property
    def purge_settings(self) -> dict[str, bool]:
        """A dictionary mapping DB item types to a boolean value indicating whether to wipe them or not,
        or None if the entire DB should suffer."""
        return self.options.get("purge_settings")

    @property
    def write_index(self) -> int:
        """The index this connection has in concurrent writing. Defaults to 1, lower writes earlier.
        If two or more connections have the same, then no order is enforced among them.
        """
        return self.options.get("write_index", 1)

    @property
    def is_filter_online_by_default(self) -> bool:
        """True if new filters should be online by default."""
        return self._filter_settings.auto_online

    def has_filters_online(self) -> bool:
        """Tests if connection has any online filters.

        Returns:
            True if there are online filters, False otherwise
        """
        return self._filter_settings.has_any_filter_online()

    def require_filter_online(self, filter_type: str) -> bool:
        """Tests if online filters of given type are required for execution.

        Args:
            filter_type: filter type

        Returns:
            True if online filters are required, False otherwise
        """
        return self._filter_settings.enabled_filter_types[filter_type] and self.options.get(
            "require_" + filter_type, False
        )

    def is_filter_type_enabled(self, filter_type: str) -> bool:
        """Tests if filter type is enabled.

        Args:
            filter_type: filter type

        Returns:
            True if filter type is enabled, False otherwise
        """
        return self._filter_settings.enabled_filter_types[filter_type]

    def notifications(self) -> list[str]:
        """See base class."""
        notifications = []
        for filter_type in (SCENARIO_FILTER_TYPE, ALTERNATIVE_FILTER_TYPE):
            filter_settings = self._filter_settings
            if self.require_filter_online(filter_type) and (
                not filter_settings.has_filter_online(filter_type)
                if filter_settings.has_filters()
                else not filter_settings.auto_online
            ):
                filter_name = {SCENARIO_FILTER_TYPE: "scenario", ALTERNATIVE_FILTER_TYPE: "alternative"}[filter_type]
                notifications.append(f"At least one {filter_name} filter must be active.")
        return notifications

    def receive_resources_from_source(self, resources):
        """See base class."""
        self._resources = {r for r in resources if r.type_ == "database" and r.filterable}

    def clean_up_backward_resources(self, resources: list[ProjectItemResource]) -> None:
        self._do_purge_before_writing(resources)

    def convert_backward_resources(
        self, resources: list[ProjectItemResource], sibling_connections: list[Connection]
    ) -> list[ProjectItemResource]:
        """Called when advertising resources through this connection *in the BACKWARD direction*.
        Takes the initial list of resources advertised by the destination item and returns a new list,
        which is the one finally advertised.

        Args:
            resources: Resources to convert
            sibling_connections: Sibling connections

        Returns:
            list of ProjectItemResource
        """
        return self._apply_use_memory_db(self._apply_write_index(resources, sibling_connections))

    def convert_forward_resources(self, resources: list[ProjectItemResource]) -> list[ProjectItemResource]:
        """Called when advertising resources through this connection *in the FORWARD direction*.
        Takes the initial list of resources advertised by the source item and returns a new list,
        which is the one finally advertised.

        Args:
            resources: Resources to convert

        Returns:
            list of ProjectItemResource
        """
        return self._apply_use_memory_db(self._apply_use_datapackage(resources))

    def _do_purge_before_writing(self, resources: list[ProjectItemResource]) -> Iterable[str]:
        if self.purge_before_writing:
            to_urls = (r.url for r in resources if r.type_ == "database")
            for url in to_urls:
                purge_url(url, self.purge_settings, self._logger)
            return to_urls
        return []

    def _apply_use_memory_db(self, resources: list[ProjectItemResource]) -> list[ProjectItemResource]:
        if not self.use_memory_db:
            return resources
        final_resources = []
        for r in resources:
            if r.type_ == "database":
                r = r.clone(additional_metadata={"memory": True})
            final_resources.append(r)
        return final_resources

    def _apply_write_index(
        self, resources: list[ProjectItemResource], sibling_connections: list[Connection]
    ) -> list[ProjectItemResource]:
        final_resources = []
        precursors = set(c.name for c in sibling_connections if c.write_index < self.write_index)
        for r in resources:
            if r.type_ == "database":
                r = r.clone(
                    additional_metadata={"current": self.name, "precursors": precursors, "part_count": PartCount()}
                )
            final_resources.append(r)
        return final_resources

    def _apply_use_datapackage(self, resources: list[ProjectItemResource]) -> list[ProjectItemResource]:
        if not self.use_datapackage:
            return resources
        # Split CSVs from the rest of resources
        final_resources = []
        csv_filepaths = []
        for r in resources:
            if r.hasfilepath:
                if os.path.splitext(r.path)[1].lower() == ".csv":
                    csv_filepaths.append(r.path)
                    continue
                if os.path.basename(r.path) == "datapackage.json":
                    continue
            final_resources.append(r)
        if not csv_filepaths:
            return final_resources
        # Build Package from CSVs and add it to the resources
        base_path = os.path.dirname(os.path.commonpath(csv_filepaths))
        package = Package(basepath=base_path)
        for path in csv_filepaths:
            package.add_resource(Resource(path=os.path.relpath(path, base_path)))
        package_path = os.path.join(base_path, "datapackage.json")
        package.to_json(package_path)
        provider = resources[0].provider_name
        package_resource = file_resource(provider, package_path, label=f"datapackage@{provider}")
        package_resource.metadata = resources[0].metadata
        final_resources.append(package_resource)
        return final_resources

    def ready_to_execute(self) -> bool:
        """See base class."""
        for filter_type in (SCENARIO_FILTER_TYPE, ALTERNATIVE_FILTER_TYPE):
            if self.require_filter_online(filter_type) and not self._filter_settings.has_filter_online(filter_type):
                return False
        return True

    def to_dict(self) -> dict:
        """Returns a dictionary representation of this Connection.

        Returns:
            dict: serialized Connection
        """
        d = super().to_dict()
        if self.options:
            d["options"] = self.options.copy()
        d["filter_settings"] = self._filter_settings.to_dict()
        return d

    @staticmethod
    def connection_dict_local_entries() -> list[tuple[str, ...]]:
        local_entries = []
        for filter_entries in FilterSettings.settings_dict_local_entries():
            local_entries.append(("filter_settings",) + filter_entries)
        return local_entries

    @staticmethod
    def _constructor_args_from_dict(connection_dict: dict) -> dict:
        """See base class."""
        kw_args = ConnectionBase._constructor_args_from_dict(connection_dict)
        kw_args["options"] = connection_dict.get("options")
        filter_settings = connection_dict.get("filter_settings")
        if filter_settings is not None:
            kw_args["filter_settings"] = FilterSettings.from_dict(filter_settings)
        else:
            disabled_names = connection_dict.get("disabled_filters")
            if disabled_names is not None:
                known_filters = _restore_legacy_disabled_filters(disabled_names)
                kw_args["filter_settings"] = FilterSettings(known_filters)
        return kw_args


class Connection(ResourceConvertingConnection):
    """Represents a connection between two project items."""

    def __init__(
        self,
        source_name: str,
        source_position: ConnectorPosition,
        destination_name: str,
        destination_position: ConnectorPosition,
        options: ConnectionOptions | None = None,
        filter_settings: FilterSettings | None = None,
    ):
        """
        Args:
            source_name: source project item's name
            source_position: source anchor's position
            destination_name: destination project item's name
            destination_position: destination anchor's position
            options: any additional options
            filter_settings: filter settings
        """
        super().__init__(source_name, source_position, destination_name, destination_position, options, filter_settings)
        self._enabled_filter_values: dict[str, dict[str, list[str] | list[list[str]]]] | None = None
        self._source_visited = False

    def visit_source(self) -> None:
        self._source_visited = True

    def visit_destination(self) -> None:
        if not self._source_visited:
            # Can happen in loop execution
            return
        self._source_visited = False
        self.emit_flash()

    def enabled_filters(self, resource_label: str) -> dict[str, list[str] | list[list[str]]]:
        """Returns enabled filter names for given resource label.

        Args:
            resource_label: resource label

        Returns:
            mapping from filter type to list of online filter names
        """
        if self._enabled_filter_values is None:
            self._prepare_enabled_filter_values()
        return self._enabled_filter_values.get(resource_label)

    def _prepare_enabled_filter_values(self) -> None:
        """Reads filter information from database."""
        self._enabled_filter_values = {}
        for resource in self._resources:
            url = resource.url
            if not url:
                continue
            try:
                with DatabaseMapping(url) as db_map:
                    known_filters = self._filter_settings.known_filters.get(resource.label, {})
                    enabled_filter_values = self._enabled_filter_values.setdefault(resource.label, {})
                    if self._filter_settings.enabled_filter_types[SCENARIO_FILTER_TYPE]:
                        enabled_scenarios = self._fetch_scenario_filter_values(db_map, known_filters)
                        if enabled_scenarios:
                            enabled_filter_values[SCENARIO_FILTER_TYPE] = enabled_scenarios
                    if self._filter_settings.enabled_filter_types[ALTERNATIVE_FILTER_TYPE]:
                        enabled_alternatives = self._fetch_alternative_filter_values(db_map, known_filters)
                        if enabled_alternatives:
                            enabled_filter_values[ALTERNATIVE_FILTER_TYPE] = enabled_alternatives
            except (SpineDBAPIError, SpineDBVersionError):
                continue

    def _fetch_scenario_filter_values(
        self, db_map: DatabaseMapping, known_filters: dict[str, dict[str, bool]]
    ) -> list[str]:
        """Fetches scenario names from database and picks the ones that are enabled by filter settings.

        Args:
            db_map: database mapping
            known_filters: mapping from filter type to filter settings

        Returns:
            scenario filter values
        """
        filter_settings = known_filters.get(SCENARIO_FILTER_TYPE, {})
        available_scenarios = {row.name for row in db_map.query(db_map.scenario_sq)}
        enabled_scenarios = set()
        for scenario_name in available_scenarios:
            if filter_settings.get(scenario_name, self._filter_settings.auto_online):
                enabled_scenarios.add(scenario_name)
        return sorted(enabled_scenarios)

    def _fetch_alternative_filter_values(
        self, db_map: DatabaseMapping, known_filters: dict[str, dict[str, bool]]
    ) -> list[list[str]]:
        """Fetches enabled alternative names from database.

        Args:
            db_map: database mapping
            known_filters: mapping from filter type to filter settings

        Returns:
            alternative filter values
        """
        filter_settings = known_filters.get(ALTERNATIVE_FILTER_TYPE, {})
        available_alternatives = {row.name for row in db_map.query(db_map.alternative_sq)}
        enabled_alternatives = set()
        for alternative_name in available_alternatives:
            if filter_settings.get(alternative_name, self._filter_settings.auto_online):
                enabled_alternatives.add(alternative_name)
        return [list(enabled_alternatives)] if enabled_alternatives else []

    @classmethod
    def from_dict(cls, connection_dict: dict) -> Connection:
        """Restores a connection from dictionary.

        Args:
            connection_dict: connection dictionary

        Returns:
            restored connection
        """
        kw_args_from_dict = cls._constructor_args_from_dict(connection_dict)
        return cls(**kw_args_from_dict)


class Jump(ConnectionBase):
    """Represents a conditional jump between two project items."""

    _DEFAULT_CONDITION: Final[dict[str, str]] = {"type": "python-script", "script": "exit(1)", "specification": ""}

    def __init__(
        self,
        source_name: str,
        source_position: ConnectorPosition,
        destination_name: str,
        destination_position: ConnectorPosition,
        condition: dict | None = None,
        cmd_line_args: Iterable[CmdLineArg] = (),
    ):
        """
        Args:
            source_name: source project item's name
            source_position: source anchor's position
            destination_name: destination project item's name
            destination_position: destination anchor's position
            condition: jump condition
            cmd_line_args: command line arguments
        """
        super().__init__(source_name, source_position, destination_name, destination_position)
        self.condition = condition if condition is not None else self._DEFAULT_CONDITION
        self._resources_from_source: set[ProjectItemResource] = set()
        self._resources_from_destination: set[ProjectItemResource] = set()
        self.cmd_line_args = list(cmd_line_args)
        self._engine: SpineEngine | None = None
        self.item_names: set[str] = set()

    def set_engine(self, engine: SpineEngine) -> None:
        self._engine = engine

    @property
    def resources(self) -> set[ProjectItemResource]:
        return self._resources_from_source | self._resources_from_destination

    def update_cmd_line_args(self, cmd_line_args: list[CmdLineArg]) -> None:
        self.cmd_line_args = cmd_line_args

    def receive_resources_from_source(self, resources: list[ProjectItemResource]) -> None:
        """See base class."""
        self._resources_from_source = set(resources)

    def receive_resources_from_destination(self, resources: list[ProjectItemResource]) -> None:
        """See base class."""
        self._resources_from_destination = set(resources)

    def is_condition_true(self, jump_counter: int) -> bool:
        """Evaluates jump condition.

        Args:
            jump_counter: how many times jump has been executed

        Returns:
            True if jump should be executed, False otherwise
        """
        iterate = False
        if self.condition["type"] == "python-script":
            iterate = self._is_python_script_condition_true(jump_counter)
        elif self.condition["type"] == "tool-specification":
            iterate = self._is_tool_specification_condition_true(jump_counter)
        if iterate:
            self.emit_flash()
            self._update_items()
        return iterate

    def _update_items(self) -> None:
        for item_name in self.item_names:
            item = self._engine.make_item(item_name, ED.NONE)
            forward_resources, backward_resources = self._engine.resources_per_item[item_name]
            item.update(forward_resources, backward_resources)

    def _is_python_script_condition_true(self, jump_counter: int) -> bool:
        script = self.condition["script"]
        if not script.strip():
            return False
        with ExitStack() as stack:
            labelled_args = labelled_resource_args(self.resources, stack)
            expanded_args = expand_cmd_line_args(self.cmd_line_args + [jump_counter], labelled_args, self._logger)
            with tempfile.TemporaryFile("w+", encoding="utf-8") as script_file:
                script_file.write(script)
                script_file.seek(0)
                python = resolve_current_python_interpreter()
                result = subprocess.run(
                    [python, "-", *expanded_args], encoding="utf-8", stdin=script_file, capture_output=True
                )
                if result.stdout:
                    self._logger.msg_proc.emit(result.stdout)
                if result.stderr:
                    self._logger.msg_proc_error.emit(result.stderr)
                return result.returncode == 0

    def _is_tool_specification_condition_true(self, jump_counter: int) -> bool:
        item_dict = {
            "type": "Tool",
            "execute_in_work": False,
            "specification": self.condition["specification"],
            "cmd_line_args": [arg.to_dict() for arg in self.cmd_line_args] + [str(jump_counter)],
        }
        condition_tool = self._engine.do_make_item(self.name, item_dict, self._logger)
        return (
            condition_tool.execute(list(self._resources_from_source), list(self._resources_from_destination), Lock())
            == ItemExecutionFinishState.SUCCESS
        )

    @classmethod
    def from_dict(cls, jump_dict: dict, **kwargs) -> Jump:
        """Restores a Jump from dictionary.

        Args:
            jump_dict: serialized jump
            **kwargs: extra keyword arguments passed to constructor

        Returns:
            restored jump
        """
        super_kw_ags = cls._constructor_args_from_dict(jump_dict)
        condition = jump_dict["condition"]
        cmd_line_args = jump_dict.get("cmd_line_args", [])
        cmd_line_args = [make_cmd_line_arg(arg) for arg in cmd_line_args]
        return cls(condition=condition, cmd_line_args=cmd_line_args, **super_kw_ags, **kwargs)

    def to_dict(self) -> dict:
        """Returns a dictionary representation of this Jump.

        Returns:
            serialized Jump
        """
        d = super().to_dict()
        d["condition"] = self.condition
        d["cmd_line_args"] = [arg.to_dict() for arg in self.cmd_line_args]
        return d


def _restore_legacy_disabled_filters(
    disabled_filter_names: dict[str, dict[str, list[str]]],
) -> dict[str, dict[str, dict[str, bool]]]:
    """Converts legacy serialized disabled filter names to known filters dict.

    Args:
        disabled_filter_names: disabled filter names with names stored as lists

    Returns:
        known filters
    """
    deprecated_types = {"tool_filter"}
    converted = {}
    for label, names_by_type in disabled_filter_names.items():
        converted_names_by_type = converted.setdefault(label, {})
        for filter_type, names in names_by_type.items():
            if filter_type not in deprecated_types:
                converted_names_by_type[filter_type] = {name: False for name in names}
    return converted
