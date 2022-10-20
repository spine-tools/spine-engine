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
Unit tests for execution_managers module.

:author: M. Marin (KTH)
:date:   12.10.2020
"""

import unittest
from unittest.mock import MagicMock
from tempfile import TemporaryDirectory
from spine_engine.execution_managers.process_execution_manager import ProcessExecutionManager
from spine_engine.execution_managers.persistent_execution_manager import PythonPersistentExecutionManager
from spine_engine.utils.execution_resources import persistent_process_semaphore


class TestStandardExecutionManager(unittest.TestCase):
    def test_run(self):
        logger = MagicMock()
        logger.msg_proc = MagicMock()
        logger.msg_proc.attach_mock(MagicMock(), "emit")
        exec_manager = ProcessExecutionManager(logger, "python", "-c", "print('hello')")
        ret = exec_manager.run_until_complete()
        logger.msg_proc.emit.assert_called_once()
        logger.msg_proc.emit.assert_called_with("hello")
        self.assertEqual(ret, 0)

    def test_run_with_error(self):
        logger = MagicMock()
        logger.msg_proc_error = MagicMock()
        logger.msg_proc_error.attach_mock(MagicMock(), "emit")
        exec_manager = ProcessExecutionManager(logger, "python", "-c", "gibberish")
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
        exec_manager = ProcessExecutionManager(logger, "unknown_program")
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
                logger, "python", "-c", "import os; print(os.getcwd())", workdir=workdir
            )
            ret = exec_manager.run_until_complete()
        logger.msg_proc.emit.assert_called_once()
        logger.msg_proc.emit.assert_called_with(f"{workdir}")
        self.assertEqual(ret, 0)


class TestPersistentExecutionManager(unittest.TestCase):
    def test_reuse_process(self):
        logger = MagicMock()
        logger.msg_warning = MagicMock()
        exec_mngr1 = PythonPersistentExecutionManager(
            logger, ["python"], ['print("hello")'], "alias", group_id="SomeGroup"
        )
        exec_mngr1.run_until_complete()
        exec_mngr2 = PythonPersistentExecutionManager(
            logger, ["python"], ['print("hello again")'], "another alias", group_id="SomeGroup"
        )
        self.assertEqual(exec_mngr1._persistent_manager, exec_mngr2._persistent_manager)
        logger.msg_warning.emit.assert_called_once()
        exec_mngr1._persistent_manager.kill_process()

    def test_do_not_reuse_unfinished_process(self):
        persistent_process_semaphore.set_limit(2)
        logger = MagicMock()
        logger.msg_warning = MagicMock()
        exec_mngr1 = PythonPersistentExecutionManager(
            logger, ["python"], ['print("hello")'], "alias", group_id="SomeGroup"
        )
        exec_mngr2 = PythonPersistentExecutionManager(
            logger, ["python"], ['print("hello again")'], "another alias", group_id="SomeGroup"
        )
        self.assertNotEqual(exec_mngr1._persistent_manager, exec_mngr2._persistent_manager)
        exec_mngr1._persistent_manager.kill_process()
        exec_mngr2._persistent_manager.kill_process()


if __name__ == '__main__':
    unittest.main()
