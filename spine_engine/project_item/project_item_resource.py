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
Provides the ProjectItemResource class.

:authors: M. Marin (KTH)
:date:   29.4.2020
"""
import copy
import uuid
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname
from spinedb_api.filters.tools import clear_filter_configs
from spinedb_api.spine_db_server import closing_spine_db_server, quick_db_checkout
from spinedb_api.spine_db_client import SpineDBClient
from ..utils.helpers import PartCount


class ProjectItemResource:
    """Class to hold a resource made available by a project item and that may be consumed by another project item.

    Attributes:
        provider_name (str): name of resource provider
        type_ (str): resource's type
        label (str): an identifier string
        metadata (dict): resource's metadata
    """

    def __init__(self, provider_name, type_, label, url=None, metadata=None, filterable=False, identifier=None):
        """
        Args:
            provider_name (str): The name of the item that provides the resource
            type_ (str): The resource type, currently available types:

                - "file": url points to the file's path
                - "file_pack": resource is part of a pack; url points to the file's path
                - "database": url is the databases url
            label (str): A label that identifies the resource.
            url (str, optional): The url of the resource.
            metadata (dict): Additional metadata providing extra information about the resource.
                Currently available keys:

                - filter_stack (str): resource's filter stack
                - filter_id (str): filter id
            filterable (bool): If True, the resource provides opportunity for filtering
            identifier (str): an identifier of the original instance, shared also by all the clones
        """
        self.provider_name = provider_name
        self.type_ = type_
        self.label = label
        self._url = url
        self._parsed_url = urlparse(self._url)
        self.metadata = metadata if metadata is not None else dict()
        self._filterable = filterable
        self._identifier = identifier if identifier is not None else uuid.uuid4().hex

    @contextmanager
    def open(self, db_checkin=False, db_checkout=False):
        if self.type_ == "database":
            ordering = {
                "id": self._identifier,
                "part_count": self.metadata.get("part_count", PartCount()),
                "current": self.metadata.get("current"),
                "precursors": self.metadata.get("precursors", set()),
            }
            with closing_spine_db_server(
                self.url, memory=self.metadata.get("memory", False), ordering=ordering
            ) as server_url:
                if db_checkin:
                    SpineDBClient.from_server_url(server_url).db_checkin()
                try:
                    yield server_url
                finally:
                    if db_checkout:
                        SpineDBClient.from_server_url(server_url).db_checkout()
        else:
            yield self.path if self.hasfilepath else ""

    def quick_db_checkout(self):
        if self.type_ != "database":
            return
        ordering = {
            "id": self._identifier,
            "part_count": self.metadata.get("part_count", PartCount()),
            "current": self.metadata.get("current"),
            "precursors": self.metadata.get("precursors", set()),
        }
        quick_db_checkout(ordering)

    def clone(self, additional_metadata=None):
        """Clones this resource and optionally updates the clone's metadata.

        Args:
            additional_metadata (dict): metadata to add to the clone

        Returns:
            ProjectItemResource: cloned resource
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
            # don't attempt to compare against unrelated types
            return NotImplemented
        return (
            self.provider_name == other.provider_name
            and self.type_ == other.type_
            and self._url == other._url
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
    def url(self):
        """Resource URL."""
        return self._url

    @url.setter
    def url(self, url):
        self._url = url
        self._parsed_url = urlparse(self._url)

    @property
    def path(self):
        """Returns the resource path in the local syntax, as obtained from parsing the url."""
        return url2pathname(self._parsed_url.path)

    @property
    def scheme(self):
        """Returns the resource scheme, as obtained from parsing the url."""
        return self._parsed_url.scheme

    @property
    def hasfilepath(self):
        if not self._url:
            return False
        return self.type_ in ("file", "file_pack") or (self.type_ == "database" and self.scheme == "sqlite")

    @property
    def arg(self):
        return self._url if self.type_ == "database" else self.path

    @property
    def filterable(self):
        return self._filterable


class CmdLineArg:
    """Command line argument for items that execute shell commands."""

    def __init__(self, arg):
        """
        Args:
            arg (str): command line argument
        """
        self.arg = arg
        self.missing = False

    def __eq__(self, other):
        if not isinstance(other, CmdLineArg):
            return NotImplemented
        return self.arg == other.arg

    def __str__(self):
        return self.arg

    def to_dict(self):
        """Serializes argument to JSON compatible dict.

        Returns:
            dict: serialized command line argument
        """
        return {"type": "literal", "arg": self.arg}


class LabelArg(CmdLineArg):
    """Command line argument that gets replaced by a project item's resource URL/file path."""

    def to_dict(self):
        """See base class."""
        return {"type": "resource", "arg": self.arg}


def database_resource(provider_name, url, label=None, filterable=False):
    """
    Constructs a database resource.

    Args:
        provider_name (str): resource provider's name
        url (str): database URL
        label (str, optional): resource label
        filterable (bool): is resource filterable
    """
    if label is None:
        label = clear_filter_configs(url)
    return ProjectItemResource(provider_name, "database", label, url, filterable=filterable)


def file_resource(provider_name, file_path, label=None):
    """
    Constructs a file resource.

    Args:
        provider_name (str): resource provider's name
        file_path (str): path to file
        label (str, optional): resource label
    """
    if label is None:
        label = file_path
    url = Path(file_path).resolve().as_uri()
    return ProjectItemResource(provider_name, "file", label, url)


def transient_file_resource(provider_name, label, file_path=None):
    """
    Constructs a transient file resource.

    Args:
        provider_name (str): resource provider's name
        label (str): resource label
        file_path (str, optional): file path if the file exists
    """
    if file_path is not None:
        url = Path(file_path).resolve().as_uri()
    else:
        url = None
    return ProjectItemResource(provider_name, "file", label, url)


def file_resource_in_pack(provider_name, label, file_path=None):
    """
    Constructs a file resource that is part of a resource pack.

    Args:
        provider_name (str): resource provider's name
        label (str): resource label
        file_path (str, optional): file path if the file exists
    """
    if file_path is not None:
        url = Path(file_path).resolve().as_uri()
    else:
        url = None
    return ProjectItemResource(provider_name, "file_pack", label, url)


def extract_packs(resources):
    """Extracts file packs from resources.

    Args:
        resources (Iterable of ProjectItemResource): resources to process

    Returns:
        tuple: list of non-pack resources and dictionary of packs keyed by label
    """
    singles = list()
    packs = dict()
    for resource in resources:
        if resource.type_ != "file_pack":
            singles.append(resource)
        else:
            packs.setdefault(resource.label, list()).append(resource)
    return singles, packs


def labelled_resource_filepaths(resources):
    """Returns a dict mapping resource labels to file paths available in given resources.
    The label acts as an identifier for a 'transient_file'.
    """
    return {resource.label: resource.path for resource in resources if resource.hasfilepath}


def get_labelled_sources(resources):
    """Organizes resources into URLs or file paths keyed by resource label.

    Args:
        resources (Iterable of ProjectItemResource): resources to organize

    Returns:
        dict: a mapping from resource label to list of URLs or file paths
    """
    d = {}
    for resource in resources:
        if resource.type_ == "database":
            d.setdefault(resource.label, []).append(resource.url)
        elif resource.hasfilepath:
            d.setdefault(resource.label, []).append(resource.path)
    return d


def make_cmd_line_arg(arg_spec):
    """Deserializes argument from dictionary.

    Args:
        arg_spec (dict or str): serialized command line argument

    Returns:
        CmdLineArg: deserialized command line argument
    """
    if not isinstance(arg_spec, dict):
        return CmdLineArg(arg_spec)
    type_ = arg_spec["type"]
    construct = {"literal": CmdLineArg, "resource": LabelArg}[type_]
    return construct(arg_spec["arg"])


def labelled_resource_args(resources, stack, db_checkin=False, db_checkout=False):
    """
    Args:
        resources (Iterable of ProjectItemResource): resources to process
        stack (ExitStack)

    Yields:
        dict: mapping from resource label to resource args.
    """
    result = {}
    single_resources, pack_resources = extract_packs(resources)
    for resource in single_resources:
        result[resource.label] = stack.enter_context(resource.open(db_checkin=db_checkin, db_checkout=db_checkout))
    for label, resources_ in pack_resources.items():
        result[label] = " ".join(
            stack.enter_context(r.open(db_checkin=db_checkin, db_checkout=db_checkout)) for r in resources_
        )
    return result


def expand_cmd_line_args(args, label_to_arg, logger):
    """Expands command line arguments by replacing resource labels by URLs/paths.

    Args:
        args (list of CmdLineArg): command line arguments
        label_to_arg (dict): a mapping from resource label to cmd line argument
        logger (LoggerInterface): a logger

    Returns:
        list of str: command line arguments as strings
    """
    expanded_args = list()
    for arg in args:
        if not isinstance(arg, LabelArg):
            expanded_args.append(str(arg))
            continue
        expanded = label_to_arg.get(str(arg))
        if expanded is None:
            logger.msg_warning.emit(f"No resources matching argument '{arg}'.")
            continue
        if expanded:
            expanded_args.append(expanded)
    return expanded_args
