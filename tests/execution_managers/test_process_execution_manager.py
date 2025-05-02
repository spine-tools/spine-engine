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
"""Unit tests for ``process_execution_manager`` module."""
import os.path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock
from spine_engine.execution_managers.process_execution_manager import ProcessExecutionManager


class TestProcessExecutionManager(unittest.TestCase):
    def test_run(self):
        logger = MagicMock()
        logger.msg_proc = MagicMock()
        logger.msg_proc.attach_mock(MagicMock(), "emit")
        exec_manager = ProcessExecutionManager(logger, "python", ["-c", "print('hello')"])
        ret = exec_manager.run_until_complete()
        logger.msg_proc.emit.assert_called_once()
        logger.msg_proc.emit.assert_called_with("hello")
        self.assertEqual(ret, 0)

    def test_run_with_error(self):
        logger = MagicMock()
        logger.msg_proc_error = MagicMock()
        logger.msg_proc_error.attach_mock(MagicMock(), "emit")
        exec_manager = ProcessExecutionManager(logger, "python", ["-c", "gibberish"])
        ret = exec_manager.run_until_complete()
        logger.msg_proc_error.emit.assert_called_with("NameError: name 'gibberish' is not defined")
        self.assertEqual(ret, 1)

    def test_run_unknown_program(self):
        logger = MagicMock()
        logger.msg_proc = MagicMock()
        logger.msg_proc_error = MagicMock()
        logger.msg_standard_execution = MagicMock()
        logger.msg_proc.attach_mock(MagicMock(), "emit")
        logger.msg_proc_error.attach_mock(MagicMock(), "emit")
        logger.msg_standard_execution.attach_mock(MagicMock(), "emit")
        exec_manager = ProcessExecutionManager(logger, "unknown_program", [])
        ret = exec_manager.run_until_complete()
        logger.msg_proc.emit.assert_not_called()
        logger.msg_proc_error.emit.assert_not_called()
        logger.msg_standard_execution.emit.assert_called_once()
        self.assertEqual(ret, 1)

    def test_run_with_workdir(self):
        logger = MagicMock()
        logger.msg_proc = MagicMock()
        logger.msg_proc.attach_mock(MagicMock(), "emit")
        logger.msg_proc_error = MagicMock()
        logger.msg_proc_error.attach_mock(MagicMock(), "emit")
        with TemporaryDirectory() as workdir:
            exec_manager = ProcessExecutionManager(
                logger, "python", ["-c", "import os; print(os.getcwd())"], workdir=workdir
            )
            ret = exec_manager.run_until_complete()
        logger.msg_proc.emit.assert_called_once()
        message_args = logger.msg_proc.emit.call_args.args
        self.assertEqual(len(message_args), 1)
        path_in_args = message_args[0]
        self.assertTrue(workdir == path_in_args or os.path.realpath(workdir) == path_in_args)
        self.assertEqual(ret, 0)


if __name__ == "__main__":
    unittest.main()
