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

"""
Unit tests for serialization module.

"""
from pathlib import Path
import sys
from tempfile import NamedTemporaryFile, TemporaryDirectory, gettempdir
import unittest
from spine_engine.utils.serialization import deserialize_path, path_in_dir, serialize_path, serialize_url


class TestSerialization(unittest.TestCase):
    def test_serialize_path_makes_relative_paths_from_paths_in_project_dir(self):
        with TemporaryDirectory() as path:
            project_dir = gettempdir()
            serialized = serialize_path(path, project_dir)
            expected_path = str(Path(path).relative_to(project_dir).as_posix())
            self.assertEqual(serialized, {"type": "path", "relative": True, "path": expected_path})

    def test_serialize_path_makes_absolute_paths_from_paths_not_in_project_dir(self):
        with TemporaryDirectory() as project_dir:
            with TemporaryDirectory() as path:
                serialized = serialize_path(path, project_dir)
                expected_path = str(Path(path).as_posix())
                self.assertEqual(serialized, {"type": "path", "relative": False, "path": expected_path})

    def test_serialize_url_makes_file_path_in_project_dir_relative(self):
        with NamedTemporaryFile(mode="r") as temp_file:
            url = "sqlite:///" + str(Path(temp_file.name).as_posix())
            project_dir = gettempdir()
            expected_path = str(Path(temp_file.name).relative_to(project_dir).as_posix())
            serialized = serialize_url(url, project_dir)
            self.assertEqual(
                serialized, {"type": "file_url", "relative": True, "path": expected_path, "scheme": "sqlite"}
            )

    def test_serialize_url_keeps_file_path_not_in_project_dir_absolute(self):
        with TemporaryDirectory() as project_dir:
            with NamedTemporaryFile(mode="r") as temp_file:
                expected_path = str(Path(temp_file.name).as_posix())
                if sys.platform == "win32":
                    url = "sqlite:///" + expected_path
                else:
                    url = "sqlite://" + expected_path
                serialized = serialize_url(url, project_dir)
                self.assertEqual(
                    serialized, {"type": "file_url", "relative": False, "path": expected_path, "scheme": "sqlite"}
                )

    def test_serialize_url_with_non_file_urls(self):
        project_dir = gettempdir()
        url = "http://www.spine-model.org/"
        serialized = serialize_url(url, project_dir)
        self.assertEqual(serialized, {"type": "url", "relative": False, "path": url})

    def test_serialize_relative_url_with_query(self):
        with NamedTemporaryFile(mode="r") as temp_file:
            url = "sqlite:///" + str(Path(temp_file.name).as_posix()) + "?filter=kol"
            project_dir = gettempdir()
            expected_path = str(Path(temp_file.name).relative_to(project_dir).as_posix())
            serialized = serialize_url(url, project_dir)
            self.assertEqual(
                serialized,
                {
                    "type": "file_url",
                    "relative": True,
                    "path": expected_path,
                    "scheme": "sqlite",
                    "query": "filter=kol",
                },
            )

    def test_deserialize_path_with_relative_path(self):
        project_dir = gettempdir()
        serialized = {"type": "path", "relative": True, "path": "subdir/file.fat"}
        deserialized = deserialize_path(serialized, project_dir)
        self.assertEqual(deserialized, str(Path(project_dir, "subdir", "file.fat")))

    def test_deserialize_path_with_absolute_path(self):
        with TemporaryDirectory() as project_dir:
            serialized = {"type": "path", "relative": False, "path": str(Path(gettempdir(), "file.fat").as_posix())}
            deserialized = deserialize_path(serialized, project_dir)
            self.assertEqual(deserialized, str(Path(gettempdir(), "file.fat")))

    def test_deserialize_path_with_relative_file_url(self):
        project_dir = gettempdir()
        serialized = {"type": "file_url", "relative": True, "path": "subdir/database.sqlite", "scheme": "sqlite"}
        deserialized = deserialize_path(serialized, project_dir)
        expected = "sqlite:///" + str(Path(project_dir, "subdir", "database.sqlite"))
        self.assertEqual(deserialized, expected)

    def test_deserialize_path_with_absolute_file_url(self):
        with TemporaryDirectory() as project_dir:
            path = str(Path(gettempdir(), "database.sqlite").as_posix())
            serialized = {"type": "file_url", "relative": False, "path": path, "scheme": "sqlite"}
            deserialized = deserialize_path(serialized, project_dir)
            expected = "sqlite:///" + str(Path(gettempdir(), "database.sqlite"))
            self.assertEqual(deserialized, expected)

    def test_deserialize_path_with_non_file_url(self):
        project_dir = gettempdir()
        serialized = {"type": "url", "path": "http://www.spine-model.org/"}
        deserialized = deserialize_path(serialized, project_dir)
        self.assertEqual(deserialized, "http://www.spine-model.org/")

    def test_deserialize_relative_url_with_query(self):
        project_dir = gettempdir()
        serialized = {
            "type": "file_url",
            "relative": True,
            "path": "subdir/database.sqlite",
            "scheme": "sqlite",
            "query": "filter=kax",
        }
        deserialized = deserialize_path(serialized, project_dir)
        expected = "sqlite:///" + str(Path(project_dir, "subdir", "database.sqlite")) + "?filter=kax"
        self.assertEqual(deserialized, expected)


class TestPathInDir(unittest.TestCase):
    @unittest.skipIf(sys.platform != "win32", "This test uses Windows paths.")
    def test_windows_paths_on_different_drives(self):
        self.assertFalse(path_in_dir(r"Z:\path\to\file.xt", r"C:\path\\"))

    @unittest.skipIf(sys.platform != "win32", "This test uses Windows paths.")
    def test_windows_paths_on_same_drive_but_different_casing(self):
        self.assertTrue(path_in_dir(r"c:\path\to\file.xt", r"C:\path\\"))

    def test_unix_paths(self):
        self.assertTrue(path_in_dir("/path/to/my/file.dat", "/path/to"))
        self.assertFalse(path_in_dir("/path/to/my/file.dat", "/another/path"))


if __name__ == "__main__":
    unittest.main()
