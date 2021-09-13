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
Unit tests for ServerMessageParser class.
:author: P. Pääkkönen (VTT)
:date:   25.8.2021
"""

import unittest

import sys
sys.path.append('./../../../spine_engine/server/util')
import os

from ServerMessageParser import ServerMessageParser
from ServerMessage import ServerMessage

class TestServerMessageParser(unittest.TestCase):


    def test_msg_parsing(self):
        with open('testMsg.txt') as f:
            content = f.read()
            msg=ServerMessageParser.parse(content)
            #print(msg)
            self.assertEqual(msg.getCommand(),"execute")
            self.assertEqual(msg.getId(),"1")
            self.assertEqual(msg.getFileNames()[0],"helloworld.zip")

    def test_msg_parsingw(self):
        with open('testMsg2.txt') as f:
            content = f.read()
            msg=ServerMessageParser.parse(content)
            #print(msg)
            self.assertEqual(msg.getCommand(),"execute")
            self.assertEqual(msg.getId(),"1")
            self.assertEqual(len(msg.getFileNames()),0)

    def test_invalid_input1(self):
        with self.assertRaises(ValueError):
            ServerMessageParser.parse("")

    def test_invalid_input2(self):
        with self.assertRaises(ValueError):
            ServerMessageParser.parse("fiuoehfoiewjkfdewjiofdj{")            

    def test_msg_nofiles(self):
        msg=ServerMessage("execute","4","{}",None)
        jsonStr=msg.toJSON()
        #print("JSON: %s"%jsonStr)
        msg=ServerMessageParser.parse(jsonStr)
        #print(msg)
        self.assertEqual(msg.getCommand(),"execute")
        self.assertEqual(msg.getId(),"4")
        self.assertEqual(msg.getData(),{})
        self.assertEqual(len(msg.getFileNames()),0)


if __name__ == '__main__':
    unittest.main()

