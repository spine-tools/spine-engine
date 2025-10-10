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

""" Unit tests for serialization module. """
from pathlib import Path
import sys
import pytest
from spine_engine.utils.serialization import (
    PathDict,
    deserialize_path,
    is_path_dict_with_file,
    path_in_dir,
    serialize_path,
)


class TestSerializePath:
    def test_serialize_path_makes_relative_paths_from_paths_in_project_dir(self, tmp_path):
        project_dir = tmp_path / "project"
        path = project_dir / "data"
        serialized = serialize_path(str(path), str(project_dir))
        expected_path = path.relative_to(project_dir).as_posix()
        assert serialized == {"type": "path", "relative": True, "path": expected_path}

    def test_serialize_path_makes_absolute_paths_from_paths_not_in_project_dir(self, tmp_path_factory):
        project_dir = tmp_path_factory.mktemp("project")
        path = tmp_path_factory.mktemp("data")
        serialized = serialize_path(str(path), str(project_dir))
        expected_path = path.as_posix()
        assert serialized == {"type": "path", "relative": False, "path": expected_path}

    def test_force_relative_path_outside_project_dir(self, tmp_path):
        project_path = tmp_path / "project"
        path = tmp_path / "data" / "file.txt"
        assert serialize_path(path, project_path, True) == {
            "type": "path",
            "relative": True,
            "path": "../data/file.txt",
        }

    def test_force_absolute_path_inside_project_dir(self, tmp_path):
        project_path = tmp_path / "project"
        path = project_path / "data" / "file.txt"
        assert serialize_path(path, project_path, False) == {"type": "path", "relative": False, "path": path.as_posix()}


class TestDeserializePath:
    def test_deserialize_path_with_relative_path(self, tmp_path):
        project_dir = tmp_path / "project"
        serialized: PathDict = {"type": "path", "relative": True, "path": "subdir/file.fat"}
        deserialized = deserialize_path(serialized, str(project_dir))
        assert deserialized == str(project_dir / "subdir" / "file.fat")

    def test_relative_path_outside_base_path(self, tmp_path):
        project_dir = tmp_path / "project"
        serialized: PathDict = {"type": "path", "relative": True, "path": "../data/file.bat"}
        deserialized = deserialize_path(serialized, str(project_dir))
        assert deserialized == str(tmp_path / "data" / "file.bat")

    def test_deserialize_path_with_absolute_path(self, tmp_path):
        project_dir = tmp_path / "project"
        serialized: PathDict = {"type": "path", "relative": False, "path": (tmp_path / "file.fat").as_posix()}
        deserialized = deserialize_path(serialized, str(project_dir))
        assert deserialized == str(tmp_path / "file.fat")

    def test_deserialize_path_with_relative_file_url(self, tmp_path):
        project_dir = tmp_path / "project"
        serialized: PathDict = {
            "type": "file_url",
            "relative": True,
            "path": "subdir/database.sqlite",
            "scheme": "sqlite",
        }
        deserialized = deserialize_path(serialized, str(project_dir))
        expected = "sqlite:///" + str(project_dir / "subdir" / "database.sqlite")
        assert deserialized == expected

    def test_deserialize_path_with_absolute_file_url(self, tmp_path_factory):
        db_path = tmp_path_factory.mktemp("data") / "database.sqlite"
        path = db_path.as_posix()
        serialized: PathDict = {"type": "file_url", "relative": False, "path": path, "scheme": "sqlite"}
        deserialized = deserialize_path(serialized, str(tmp_path_factory.mktemp("project")))
        expected = "sqlite:///" + str(db_path)
        assert deserialized == expected

    def test_deserialize_path_with_non_file_url(self, tmp_path):
        serialized: PathDict = {"type": "url", "path": "http://www.spine-model.org/"}
        deserialized = deserialize_path(serialized, str(tmp_path))
        assert deserialized == "http://www.spine-model.org/"

    def test_deserialize_relative_url_with_query(self, tmp_path):
        project_dir = tmp_path / "project"
        serialized: PathDict = {
            "type": "file_url",
            "relative": True,
            "path": "subdir/database.sqlite",
            "scheme": "sqlite",
            "query": "filter=kax",
        }
        deserialized = deserialize_path(serialized, str(project_dir))
        expected = "sqlite:///" + str(Path(project_dir, "subdir", "database.sqlite")) + "?filter=kax"
        assert deserialized == expected


class TestPathInDir:
    @pytest.mark.skipif(sys.platform != "win32", reason="This test uses Windows paths.")
    def test_windows_paths_on_different_drives(self):
        assert not path_in_dir(r"Z:\path\to\file.xt", r"C:\path\\")

    @pytest.mark.skipif(sys.platform != "win32", reason="This test uses Windows paths.")
    def test_windows_paths_on_same_drive_but_different_casing(self):
        assert path_in_dir(r"c:\path\to\file.xt", r"C:\path\\")

    def test_unix_paths(self):
        assert path_in_dir("/path/to/my/file.dat", "/path/to")
        assert not path_in_dir("/path/to/my/file.dat", "/another/path")


class TestIsPathDictWithFile:
    def test_positives(self):
        assert is_path_dict_with_file({"type": "path", "path": "/home/bungo/data.dat", "relative": False})
        assert is_path_dict_with_file({"type": "path", "path": "data.dat", "relative": True})
        assert is_path_dict_with_file(
            {"type": "file_url", "path": "c:/users/bungo/data.sqlite", "relative": False, "scheme": "sqlite"}
        )
        assert is_path_dict_with_file({"type": "file_url", "path": "data.sqlite", "relative": True, "scheme": "sqlite"})

    def test_negatives(self):
        assert not is_path_dict_with_file({})
        assert not is_path_dict_with_file({"path": "/usr/bin/spinetoolbox"})
        assert not is_path_dict_with_file({"type": "url", "path": "www.example.com"})
