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
from spine_engine.project.connection import Connection
from spine_engine.project.filter_settings import FilterSettings


class TestConnection:
    def test_serialization(self):
        connection = Connection()
        connection_dict = connection.to_dict()
        deserialized = Connection.from_dict(connection_dict)
        assert deserialized == connection

    def test_serialization_with_filter_settings(self):
        connection = Connection(FilterSettings())
        connection_dict = connection.to_dict()
        deserialized = Connection.from_dict(connection_dict)
        assert deserialized == connection
