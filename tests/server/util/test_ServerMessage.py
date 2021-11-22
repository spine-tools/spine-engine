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
Unit tests for ServerMessage class.
:author: P. Pääkkönen (VTT)
:date:   25.8.2021
"""

import unittest
from spine_engine.server.util.server_message import ServerMessage


class TestServerMessage(unittest.TestCase):
    def test_msg_creation(self):
        listFiles = ["dffd.zip", "fdeef.zip"]
        msg = ServerMessage("execute", "4", "[]", listFiles)
        self.assertEqual("execute", msg.getCommand())
        self.assertEqual("4", msg.getId())
        self.assertEqual(listFiles, msg.getFileNames())
        self.assertEqual("[]", msg.getData())

    def test_msg_creation2_nofilenames(self):
        msg = ServerMessage("execute", "4", "[]", None)
        self.assertEqual("execute", msg.getCommand())
        self.assertEqual("4", msg.getId())
        self.assertEqual("[]", msg.getData())

    def test_msg_nodata(self):
        listFiles = ["dffd.zip", "fdeef.zip"]
        msg = ServerMessage("execute", "4", "", listFiles)
        self.assertEqual("execute", msg.getCommand())
        self.assertEqual("4", msg.getId())

    def test_message_tojson1(self):
        listFiles = ["dffd.zip", "fdeef.zip"]
        msg = ServerMessage("execute", "4", "[]", listFiles)
        jsonStr = msg.toJSON()
        # print("parsed msg:\n")
        # print(jsonStr)

    def test_message_tojson2(self):
        msg = ServerMessage("execute", "4", "[]", None)
        jsonStr = msg.toJSON()
        # print("parsed msg2:\n")
        # print(jsonStr)


if __name__ == '__main__':
    unittest.main()
