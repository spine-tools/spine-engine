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
Unit tests for ``project_item_resource`` module.

:authors: A. Soininen
:date:    8.8.2022
"""
from pathlib import Path
import unittest
from spine_engine.project_item.project_item_resource import (
    database_resource,
    file_resource,
    file_resource_in_pack,
    get_labelled_sources,
)


class TestGetLabelledSources(unittest.TestCase):
    def test_empty_input_produces_empty_output(self):
        self.assertEqual(get_labelled_sources([]), {})

    def test_single_database_resource_gets_collected(self):
        resources = [database_resource("provider", "sqlite:///file.sqlite", "my database")]
        expected = {"my database": ["sqlite:///file.sqlite"]}
        self.assertEqual(get_labelled_sources(resources), expected)

    def test_single_file_resource_gets_collected(self):
        resources = [file_resource("provider", "/path/to/file", "my file")]
        expected = {"my file": [str(Path("/", "path", "to", "file").resolve())]}
        self.assertEqual(get_labelled_sources(resources), expected)

    def test_multiple_file_resources_in_packs_get_collected(self):
        resources = [
            file_resource_in_pack("provider", "*.dat", "/path/file1.dat"),
            file_resource_in_pack("provider", "*.dat", "/path/file2.dat"),
        ]
        expected = {
            "*.dat": [str(Path("/", "path", "file1.dat").resolve()), str(Path("/", "path", "file2.dat").resolve())]
        }
        self.assertEqual(get_labelled_sources(resources), expected)


if __name__ == '__main__':
    unittest.main()
