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
Unit tests for FileExtractor class.
:author: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   23.8.2021
"""

import unittest
import os
from pathlib import Path
from spine_engine.server.util.zip_handler import ZipHandler


class TestZipHandler(unittest.TestCase):
    def test_simple_extraction(self):
        zip_file_path = str(Path(__file__).parent / "test_zipfile.zip")
        output_dir_path = str(Path(__file__).parent / "output")
        ZipHandler.extract(zip_file_path, output_dir_path)
        self.assertEqual(os.path.isdir(output_dir_path), True)

    def test_invalid_input1(self):
        with self.assertRaises(ValueError):
            ZipHandler.extract("", "./output")

    def test_invalid_input2(self):
        with self.assertRaises(ValueError):
            ZipHandler.extract("does_not_exist.zip", "")

    def test_removeFolder(self):
        zip_file_path = str(Path(__file__).parent / "test_zipfile.zip")
        output_dir_path = str(Path(__file__).parent / "output")
        ZipHandler.extract(zip_file_path, output_dir_path)
        self.assertEqual(os.path.isdir(output_dir_path), True)
        ZipHandler.delete_folder(output_dir_path)
        self.assertEqual(os.path.isdir(output_dir_path), False)

    def test_remove_nonexisting_Folder(self):
        with self.assertRaises(ValueError):
            ZipHandler.delete_folder("./output2")

    def test_invalid_input1_remove_folder(self):
        with self.assertRaises(ValueError):
            ZipHandler.delete_folder("")

    def test_package_and_delete_file(self):
        src = os.path.join(str(Path(__file__).parent), "projectforpackagingtests")
        dst = os.path.join(src, os.pardir)
        ZipHandler.package(src, dst, "packager_test_zip")
        zip_file = os.path.join(dst, "packager_test_zip.zip")
        self.assertTrue(os.path.isfile(zip_file))
        os.remove(zip_file)
        self.assertFalse(os.path.isfile(zip_file))


if __name__ == "__main__":
    unittest.main()
