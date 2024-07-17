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

"""Contains project item specification class."""
import json
from spine_engine.utils.helpers import gather_leaf_data, shorten


class ProjectItemSpecification:
    """
    Class to hold a project item specification.

    Attributes:
        item_type (str): type of the project item the specification is compatible with
        definition_file_path (str): specification's JSON file path
    """

    def __init__(self, name, description=None, item_type=""):
        """
        Args:
            name (str): specification name
            description (str): description
            item_type (str): Project item type
        """
        self.name = name
        self.short_name = shorten(name)
        self.description = description
        self.item_type = item_type
        self.definition_file_path = ""
        self.plugin = None

    # TODO: Needed?
    def set_name(self, name):
        """Set object name and short name.
        Note: Check conflicts (e.g. name already exists)
        before calling this method.

        Args:
            name (str): New (long) name for this object
        """
        self.name = name
        self.short_name = shorten(name)

    # TODO:Needed?
    def set_description(self, description):
        """Set object description.

        Args:
            description (str): Object description
        """
        self.description = description

    def save(self):
        """
        Writes the specification to the path given by ``self.definition_file_path``

        Returns:
            dict: definition's local entries
        """
        definition = self.to_dict()
        local_entries = self._definition_local_entries()
        popped = gather_leaf_data(definition, local_entries, pop=True)
        with open(self.definition_file_path, "w") as fp:
            json.dump(definition, fp, indent=4)
        return popped

    def to_dict(self):
        """
        Returns a dict for the specification.

        Returns:
            dict: specification dict
        """
        raise NotImplementedError()

    def local_data(self):
        """Makes a dict out of specification's local data.

        Returns:
            dict: local data
        """
        return gather_leaf_data(self.to_dict(), self._definition_local_entries())

    def may_have_local_data(self):
        """Tests if specification could have project specific local data.

        Returns:
            bool: True if specification may have local data, False otherwise
        """
        return bool(self._definition_local_entries())

    @staticmethod
    def _definition_local_entries():
        """Returns entries or 'paths' in specification dict that should be stored in project's local data directory.

        Returns:
            list of tuple of str: local data item dict entries
        """
        return []

    def is_equivalent(self, other):
        """
        Returns True if two specifications are essentially the same.

        Args:
            other (DataTransformerSpecification): specification to compare to

        Returns:
            bool: True if the specifications are equivalent, False otherwise
        """
        raise NotImplementedError()
