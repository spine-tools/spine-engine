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
"""Unit tests for ``persistent_execution_manager`` module."""
import unittest
from unittest.mock import MagicMock
from spine_engine.execution_managers.persistent_execution_manager import PythonPersistentExecutionManager
from spine_engine.utils.execution_resources import persistent_process_semaphore


class TestPythonPersistentExecutionManager(unittest.TestCase):
    def test_reuse_process(self):
        logger = MagicMock()
        exec_mngr1 = PythonPersistentExecutionManager(
            logger, ["python"], ['print("hello")'], "alias", kill_completed_processes=False, group_id="SomeGroup"
        )
        exec_mngr1.run_until_complete()
        exec_mngr2 = PythonPersistentExecutionManager(
            logger,
            ["python"],
            ['print("hello again")'],
            "another alias",
            kill_completed_processes=False,
            group_id="SomeGroup",
        )
        self.assertEqual(exec_mngr1._persistent_manager, exec_mngr2._persistent_manager)
        logger.msg_warning.emit.assert_called_once()
        exec_mngr1._persistent_manager.kill_process()

    def test_do_not_reuse_unfinished_process(self):
        persistent_process_semaphore.set_limit(2)
        logger = MagicMock()
        exec_mngr1 = PythonPersistentExecutionManager(
            logger, ["python"], ['print("hello")'], "alias", kill_completed_processes=False, group_id="SomeGroup"
        )
        exec_mngr2 = PythonPersistentExecutionManager(
            logger,
            ["python"],
            ['print("hello again")'],
            "another alias",
            kill_completed_processes=False,
            group_id="SomeGroup",
        )
        self.assertNotEqual(exec_mngr1._persistent_manager, exec_mngr2._persistent_manager)
        exec_mngr1._persistent_manager.kill_process()
        exec_mngr2._persistent_manager.kill_process()

    def test_failing_process(self):
        logger = MagicMock()
        exec_mngr = PythonPersistentExecutionManager(
            logger, ["python"], ["exit(666)"], "my execution", kill_completed_processes=False, group_id="SomeGroup"
        )
        self.assertEqual(exec_mngr.run_until_complete(), -1)
        self.assertTrue(exec_mngr.killed)
        expected_messages = [
            {"type": "persistent_started", "language": "python"},
            {"type": "execution_started", "args": "python"},
            {"type": "stdin", "data": "# Running my execution"},
            {"type": "stdout", "data": "Kernel died (×_×)"},
        ]
        message_emits = logger.msg_persistent_execution.emit.call_args_list
        self.assertEqual(len(message_emits), len(expected_messages))
        for call, expected_message in zip(message_emits, expected_messages):
            for key, expected in expected_message.items():
                self.assertEqual(call[0][0][key], expected)


if __name__ == "__main__":
    unittest.main()
