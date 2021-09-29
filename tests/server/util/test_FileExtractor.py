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
:author: P. Pääkkönen (VTT)
:date:   23.8.2021
"""

import unittest
import os
from spine_engine.server.util.FileExtractor import FileExtractor


class TestFileExtractor(unittest.TestCase):

    def test_simple_extraction(self):
        FileExtractor.extract(os.path.join(os.getcwd(), "tests", "server", "util", "test_zipfile.zip"), "./output")
        self.assertEqual(os.path.isdir("./output"), True)
        print("test_simple_extraction")

    def test_invalid_input1(self):
        with self.assertRaises(ValueError):
            FileExtractor.extract("", "./output")

    def test_invalid_input2(self):
        with self.assertRaises(ValueError):
            FileExtractor.extract("test_zipfile.zip", "")

    def test_removeFolder(self):
        FileExtractor.extract(os.path.join(os.getcwd(), "tests", "server", "util", "test_zipfile.zip"), "./output")
        self.assertEqual(os.path.isdir("./output"), True)
        FileExtractor.deleteFolder("./output")
        self.assertEqual(os.path.isdir("./output"), False)

    def test_remove_nonexisting_Folder(self):
        with self.assertRaises(ValueError):
            FileExtractor.deleteFolder("./output2")

    def test_invalid_input1_remove_folder(self):
        with self.assertRaises(ValueError):
            FileExtractor.deleteFolder("")


if __name__ == "__main__":
    unittest.main()

