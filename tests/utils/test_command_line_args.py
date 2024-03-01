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

""" Unit tests for command line args module. """

import unittest
from spine_engine.utils.command_line_arguments import split_cmdline_args


class TestToolSpecification(unittest.TestCase):
    def test_split_cmdline_args(self):
        splitted = split_cmdline_args("")
        self.assertFalse(bool(splitted))
        splitted = split_cmdline_args("--version")
        self.assertEqual(splitted, ["--version"])
        splitted = split_cmdline_args("--input=data.dat -h 5")
        self.assertEqual(splitted, ["--input=data.dat", "-h", "5"])
        splitted = split_cmdline_args('--output="a long file name.txt"')
        self.assertEqual(splitted, ["--output=a long file name.txt"])
        splitted = split_cmdline_args("--file='file name with spaces.dat' -i 3")
        self.assertEqual(splitted, ["--file=file name with spaces.dat", "-i", "3"])
        splitted = split_cmdline_args("'quotation \"within\" a quotation'")
        self.assertEqual(splitted, ['quotation "within" a quotation'])
        splitted = split_cmdline_args("  ")
        self.assertEqual(splitted, [])
        splitted = split_cmdline_args("-a  -b")
        self.assertEqual(splitted, ["-a", "-b"])


if __name__ == "__main__":
    unittest.main()
