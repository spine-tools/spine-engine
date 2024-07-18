#####################################################################################################################
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
Unit tests for start_server.py script.
"""
from multiprocessing import Process
import time
import unittest
from spine_engine.server.start_server import main


class TestStartServer(unittest.TestCase):
    def test_start_server(self):
        server = Process(target=main, args=(["", "5001"],))
        server.start()
        self.assertTrue(server.is_alive())
        time.sleep(5)  # Starting the server takes some time (in tests)
        server.terminate()
        server.join()  # process is still alive after terminate if we don't join()
        self.assertFalse(server.is_alive())
        self.assertTrue(server.exitcode == 0)

    def test_start_server_with_security_folder_missing(self):
        # Expected behaviour is to exit immediately and print an error message
        server = Process(target=main, args=(["", "5001", "stonehouse", ""],))
        server.start()
        server.join()
        self.assertTrue(server.exitcode == 0)

    def test_invalid_number_of_args(self):
        server = Process(target=main, args=(["", "a", "b"],))
        server.start()
        server.join()
        self.assertTrue(server.exitcode == 0)


if __name__ == "__main__":
    unittest.main()
