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
"""Provides the ProjectItemResource class."""
from __future__ import annotations
from collections.abc import Iterable, Iterator
from contextlib import ExitStack, contextmanager
import copy
from pathlib import Path
from typing import Literal, TypeAlias, TypedDict
from urllib.parse import urlparse
from urllib.request import url2pathname
import uuid
from typing_extensions import NotRequired
from spinedb_api.filters.tools import clear_filter_configs
from spinedb_api.spine_db_client import SpineDBClient
from spinedb_api.spine_db_server import closing_spine_db_server, quick_db_checkout
from ..logger_interface import LoggerInterface
from ..utils.helpers import PartCount, urls_equal

ResourceType: TypeAlias = Literal["file", "directory", "file_pack", "database", "url"]


class ResourceMetadata(TypedDict):
    filter_stack: NotRequired[tuple[dict, ...]]
    filter_id: NotRequired[str]
    schema: NotRequired[str]
    part_count: NotRequired[PartCount]
    current: NotRequired[str]
    precursors: NotRequired[set[str]]


class ProjectItemResource:
    """Class to hold a resource made available by a project item and that may be consumed by another project item.

    Attributes:
        provider_name (str): name of resource provider
        type_ (str): resource's type
        label (str): an identifier string
        metadata (dict): resource's metadata
    """

    def __init__(
        self,
        provider_name: str,
        type_: ResourceType,
        label: str,
        url: str | None = None,
        metadata: ResourceMetadata | None = None,
        filterable: bool = False,
        identifier: str | None = None,
    ):
        """
        Args:
            provider_name: The name of the item that provides the resource
            type_: The resource type, currently available types:

                - "file": url points to a local file
                - "file_pack": resource is part of a pack; url points to the file's path
                - "database": url is a Spine database url
                - "url": url is a generic URL
            label: A label that identifies the resource.
            url: The url of the resource.
            metadata: Additional metadata providing extra information about the resource.
                Currently available keys:

                - filter_stack: resource's filter stack
                - filter_id: filter id
                - schema: database schema if resource is a database resource
            filterable: If True, the resource provides opportunity for filtering
            identifier: an identifier of the original instance, shared also by all the clones
        """
        self.provider_name = provider_name
        self.type_: ResourceType = type_
        self.label = label
        self._url = url
        self._filepath: str | None = None
        self._parsed_url = urlparse(self._url)
        self.metadata = metadata if metadata is not None else {}
        self._filterable = filterable
        self._identifier = identifier if identifier is not None else uuid.uuid4().hex

    @contextmanager
    def open(self, db_checkin: bool = False, db_checkout: bool = False) -> Iterator[str]:
        if self.type_ == "database":
            ordering = {
                "id": self._identifier,
                "part_count": self.metadata.get("part_count", PartCount()),
                "current": self.metadata.get("current"),
                "precursors": self.metadata.get("precursors", set()),
            }
            db_server_manager_queue = self.metadata["db_server_manager_queue"]
            with closing_spine_db_server(
                self.url,
                memory=self.metadata.get("memory", False),
                ordering=ordering,
                server_manager_queue=db_server_manager_queue,
            ) as server_url:
                if db_checkin:
                    SpineDBClient.from_server_url(server_url).db_checkin()
                try:
                    yield server_url
                finally:
                    if db_checkout:
                        SpineDBClient.from_server_url(server_url).db_checkout()
        elif self.type_ == "url":
            yield self.url
        else:
            yield self.path if self.hasfilepath else ""

    def quick_db_checkout(self) -> None:
        if self.type_ != "database":
            return
        db_server_manager_queue = self.metadata["db_server_manager_queue"]
        ordering = {
            "id": self._identifier,
            "part_count": self.metadata.get("part_count", PartCount()),
            "current": self.metadata.get("current"),
            "precursors": self.metadata.get("precursors", set()),
        }
        quick_db_checkout(db_server_manager_queue, ordering)

    def clone(self, additional_metadata: ResourceMetadata | None = None) -> ProjectItemResource:
        """Clones this resource and optionally updates the clone's metadata.

        Args:
            additional_metadata: metadata to add to the clone

        Returns:
            cloned resource
        """
        if additional_metadata is None:
            additional_metadata = {}
        metadata = copy.deepcopy(self.metadata)
        metadata.update(additional_metadata)
        return ProjectItemResource(
            self.provider_name,
            self.type_,
            label=self.label,
            url=self._url,
            metadata=metadata,
            filterable=self._filterable,
            identifier=self._identifier,
        )

    def __eq__(self, other):
        if not isinstance(other, ProjectItemResource):
            return NotImplemented
        return (
            self.provider_name == other.provider_name
            and self.type_ == other.type_
            and (
                urls_equal(self._url, other._url)
                if self._url is not None and other._url is not None
                else self._url == other._url
            )
            and self.metadata == other.metadata
            and self._filterable == other._filterable
        )

    def __hash__(self):
        return hash(repr(self))

    def __repr__(self):
        result = "ProjectItemResource("
        result += f"provider={self.provider_name}, "
        result += f"type_={self.type_}, "
        result += f"url={self._url}, "
        result += f"metadata={self.metadata}, "
        result += f"filterable={self._filterable})"
        return result

    @property
    def url(self) -> str:
        """Resource URL."""
        return self._url

    @url.setter
    def url(self, url: str) -> None:
        self._url = url
        self._parsed_url = urlparse(self._url)

    @property
    def path(self) -> str:
        """Returns the resource path in the local syntax, as obtained from parsing the url."""
        if not self._filepath:
            self._filepath = url2pathname(self._parsed_url.path) if self._parsed_url.path else ""
        return self._filepath

    @property
    def scheme(self) -> str:
        """Returns the resource scheme, as obtained from parsing the url."""
        return self._parsed_url.scheme

    @property
    def hasfilepath(self) -> bool:
        if not self._url:
            return False
        return self.type_ in ("file", "directory", "file_pack") or (
            self.type_ == "database" and self.scheme == "sqlite"
        )

    @property
    def arg(self) -> str:
        return self._url if self.type_ == "database" else self.path

    @property
    def filterable(self) -> bool:
        return self._filterable


class CmdLineArg:
    """Command line argument for items that execute shell commands."""

    def __init__(self, arg: str):
        """
        Args:
            arg: command line argument
        """
        self.arg = arg
        self.missing = False

    def __eq__(self, other):
        if not isinstance(other, CmdLineArg):
            return NotImplemented
        return self.arg == other.arg

    def __str__(self):
        return self.arg

    def to_dict(self) -> dict:
        """Serializes argument to JSON compatible dict.

        Returns:
            serialized command line argument
        """
        return {"type": "literal", "arg": self.arg}


class LabelArg(CmdLineArg):
    """Command line argument that gets replaced by a project item's resource URL/file path."""

    def to_dict(self) -> dict:
        """See base class."""
        return {"type": "resource", "arg": self.arg}


def database_resource(
    provider_name: str, url: str, label: str | None = None, filterable: bool = False, schema: str | None = None
) -> ProjectItemResource:
    """
    Constructs a Spine database resource.

    Args:
        provider_name: resource provider's name
        url: database URL
        label: resource label
        filterable: is resource filterable
        schema: database schema

    Returns:
        Spine database resource
    """
    if label is None:
        label = clear_filter_configs(url)
    metadata = None if not schema else {"schema": schema}
    return ProjectItemResource(provider_name, "database", label, url, metadata=metadata, filterable=filterable)


def url_resource(provider_name: str, url: str, label: str, schema: str | None = None) -> ProjectItemResource:
    """
    Constructs a generic URL resource.

    Args:
        provider_name: resource provider's name
        url: database URL
        label: resource label
        schema: database schema if URL is a database URL
    """
    metadata = None if not schema else {"schema": schema}
    return ProjectItemResource(provider_name, "url", label, url, metadata=metadata)


def file_resource(provider_name: str, file_path: str, label: str | None = None) -> ProjectItemResource:
    """
    Constructs a file resource.

    Args:
        provider_name: resource provider's name
        file_path: path to file
        label: resource label
    """
    if label is None:
        label = file_path
    url = Path(file_path).resolve().as_uri()
    return ProjectItemResource(provider_name, "file", label, url)


def directory_resource(provider_name: str, path: str, label: str | None = None) -> ProjectItemResource:
    """
    Constructs a directory resource.

    Args:
        provider_name: resource provider's name
        path: directory path
        label: resource label
    """
    if label is None:
        label = path
    url = Path(path).resolve().as_uri()
    return ProjectItemResource(provider_name, "directory", label, url)


def transient_file_resource(provider_name: str, label: str, file_path: str | None = None) -> ProjectItemResource:
    """
    Constructs a transient file resource.

    Args:
        provider_name: resource provider's name
        label: resource label
        file_path: file path if the file exists
    """
    if file_path is not None:
        url = Path(file_path).resolve().as_uri()
    else:
        url = None
    return ProjectItemResource(provider_name, "file", label, url)


def file_resource_in_pack(provider_name: str, label: str, file_path: str | None = None) -> ProjectItemResource:
    """
    Constructs a file resource that is part of a resource pack.

    Args:
        provider_name: resource provider's name
        label: resource label
        file_path: file path if the file exists
    """
    if file_path is not None:
        url = Path(file_path).resolve().as_uri()
    else:
        url = None
    return ProjectItemResource(provider_name, "file_pack", label, url)


def extract_packs(
    resources: Iterable[ProjectItemResource],
) -> tuple[list[ProjectItemResource], dict[str, list[ProjectItemResource]]]:
    """Extracts file packs from resources.

    Args:
        resources: resources to process

    Returns:
        list of non-pack resources and dictionary of packs keyed by label
    """
    singles = []
    packs = {}
    for resource in resources:
        if resource.type_ != "file_pack":
            singles.append(resource)
        else:
            packs.setdefault(resource.label, []).append(resource)
    return singles, packs


def labelled_resource_filepaths(resources: Iterable[ProjectItemResource]) -> dict[str, str]:
    """Returns a dict mapping resource labels to file paths available in given resources.
    The label acts as an identifier for a 'transient_file'.
    """
    return {resource.label: resource.path for resource in resources if resource.hasfilepath}


_DATABASE_RESOURCE_TYPES = ("database", "url")


def get_labelled_source_resources(resources: Iterable[ProjectItemResource]) -> dict[str, list[ProjectItemResource]]:
    """Collects URL and file resources and keys them by resource label.

    Args:
        resources : resources to organize

    Returns:
        a mapping from resource label to list of resources with URLs or file paths
    """
    d = {}
    for resource in resources:
        if resource.type_ in _DATABASE_RESOURCE_TYPES or resource.hasfilepath:
            d.setdefault(resource.label, []).append(resource)
    return d


def get_source(resource: ProjectItemResource) -> str | None:
    """Gets source from resource.

    Args:
        resource: resource

    Returns:
         source file path or URL or None if source is not available
    """
    if resource.type_ in _DATABASE_RESOURCE_TYPES:
        return resource.url
    elif resource.hasfilepath:
        return resource.path
    return None


class SourceExtras(TypedDict):
    schema: NotRequired[str | None]


def get_source_extras(resource: ProjectItemResource) -> SourceExtras:
    """Gets additional source settings from resource.

    Args:
        resource: resource

    Returns:
        additional source settings
    """
    if resource.type_ in _DATABASE_RESOURCE_TYPES:
        return {"schema": resource.metadata.get("schema")}
    return {}


def make_cmd_line_arg(arg_spec: dict | str) -> CmdLineArg:
    """Deserializes argument from dictionary.

    Args:
        arg_spec: serialized command line argument

    Returns:
        deserialized command line argument
    """
    if not isinstance(arg_spec, dict):
        return CmdLineArg(arg_spec)
    type_ = arg_spec["type"]
    construct = {"literal": CmdLineArg, "resource": LabelArg}[type_]
    return construct(arg_spec["arg"])


def labelled_resource_args(
    resources: Iterable[ProjectItemResource], stack: ExitStack, db_checkin: bool = False, db_checkout: bool = False
) -> dict[str, list[str]]:
    """Generates command line arguments for each resource.

    Args:
        resources: resources to process
        stack: context manager to ensure resources get closed properly
        db_checkin: is database checkin required
        db_checkout: is database checkout required

    Returns:
        mapping from resource label to a list of resource args.
    """
    result = {}
    single_resources, pack_resources = extract_packs(resources)
    for resource in single_resources:
        result[resource.label] = [stack.enter_context(resource.open(db_checkin=db_checkin, db_checkout=db_checkout))]
    for label, resources_ in pack_resources.items():
        result[label] = [
            stack.enter_context(r.open(db_checkin=db_checkin, db_checkout=db_checkout)) for r in resources_
        ]
    return result


def expand_cmd_line_args(args: list, label_to_arg: dict[str, list[str]], logger: LoggerInterface) -> list[str]:
    """Expands command line arguments by replacing resource labels by URLs/paths.

    Args:
        args: command line arguments
        label_to_arg: a mapping from resource label to list of cmd line arguments
        logger: a logger

    Returns:
        command line arguments as strings
    """
    expanded_args = list()
    for arg in args:
        if not isinstance(arg, LabelArg):
            expanded_args.append(str(arg))
            continue
        expanded = label_to_arg.get(str(arg))
        if expanded is None or any(not arg for arg in expanded):
            logger.msg_warning.emit(f"No resources matching argument '{arg}'.")
            continue
        expanded_args.extend(expanded)
    return expanded_args
