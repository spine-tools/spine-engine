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
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname
from spinedb_api.filters.tools import clear_filter_configs


class ProjectItemResource:
    """Class to hold a resource made available by a project item and that may be consumed by another project item.

    Attributes:
        provider_name (str): name of resource provider
        type_ (str): resource's type
        label (str): an identifier string
        metadata (dict): resource's metadata
    """

    def __init__(self, provider_name, type_, label, url=None, metadata=None):
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
        """
        self.provider_name = provider_name
        self.type_ = type_
        self.label = label
        self._url = url
        self._parsed_url = urlparse(self._url)
        self.metadata = metadata if metadata is not None else dict()

    def clone(self, additional_metadata=None):
        """ Clones a resource and optionally updates the clone's metadata.

        Args:
            additional_metadata (dict): metadata to add to the clone

        Returns:
            ProjectItemResource: cloned resource
        """
        if additional_metadata is None:
            additional_metadata = {}
        metadata = copy.deepcopy(self.metadata)
        metadata.update(additional_metadata)
        return ProjectItemResource(self.provider_name, self.type_, label=self.label, url=self._url, metadata=metadata)

    def __eq__(self, other):
        if not isinstance(other, ProjectItemResource):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return (
            self.provider_name == other.provider_name
            and self.type_ == other.type_
            and self._url == other._url
            and self.metadata == other.metadata
        )

    def __hash__(self):
        return hash(repr(self))

    def __repr__(self):
        result = "ProjectItemResource("
        result += f"provider={self.provider_name}, "
        result += f"type_={self.type_}, "
        result += f"url={self._url}, "
        result += f"metadata={self.metadata})"
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


def database_resource(provider_name, url, label=None):
    """
    Constructs a database resource.

    Args:
        provider_name (str): resource provider's name
        url (str): database URL
        label (str, optional): resource label
    """
    if label is None:
        label = clear_filter_configs(url)
    return ProjectItemResource(provider_name, "database", label, url)


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
