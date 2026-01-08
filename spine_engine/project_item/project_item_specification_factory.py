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

"""Contains project item specification factory."""
from spine_engine.logger_interface import LoggerInterface
from spine_engine.project_item.project_item_specification import ProjectItemSpecification
from spine_engine.utils.helpers import AppSettings


class ProjectItemSpecificationFactory:
    """A factory to make project item specifications."""

    @staticmethod
    def item_type() -> str:
        """Returns the project item's type."""
        raise NotImplementedError()

    @staticmethod
    def make_specification(
        definition: dict, app_settings: AppSettings, logger: LoggerInterface
    ) -> ProjectItemSpecification:
        """
        Makes a project item specification.

        Args:
            definition: specification's definition dictionary
            app_settings: Toolbox settings
            logger: A logger instance.

        Returns:
            a specification built from the given definition
        """
        raise NotImplementedError()
