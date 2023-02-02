######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""
Provides the ProjectItemInfo class.

:authors: A. Soininen (VTT)
:date:   29.4.2020
"""


class ProjectItemInfo:
    @staticmethod
    def item_category():
        """
        Returns the item category string, e.g., "Tools".

        Returns:
            str: item's category
        """
        raise NotImplementedError()

    @staticmethod
    def item_type():
        """
        Returns the item type string, e.g., "Importer".

        Returns:
            str: item's type
        """
        raise NotImplementedError()
