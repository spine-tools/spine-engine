######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""Unit tests for ``kernel_execution_manager`` module."""
import os
import unittest
import tempfile
from unittest.mock import MagicMock
from spine_engine.execution_managers.kernel_execution_manager import KernelExecutionManager, _kernel_manager_factory
from jupyter_client.kernelspec import NATIVE_KERNEL_NAME  # =='python3'


class TestKernelExecutionManager(unittest.TestCase):
    @staticmethod
    def release_exec_mngr_resources(mngr):
        """Frees resources after exec_mngr has been used. Consider putting
        this to KernelExecutionManager.close() or something."""
        if not mngr._kernel_client.context.closed:
            mngr._kernel_client.context.term()  # ResourceWarning: Unclosed <zmq.Context() happens without this
        mngr.std_out.close()  # Prevents ResourceWarning: unclosed file <_io.TextIOWrapper name='nul' ...
        mngr.std_err.close()  # Prevents ResourceWarning: unclosed file <_io.TextIOWrapper name='nul' ...

    def test_kernel_execution_manager(self):
        logger = MagicMock()
        logger.msg_kernel_execution.filter_id = ""
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as script_file:
            script_file.write('print("hello")')
            script_file.seek(0)
            d, fname = os.path.split(script_file.name)
            cmds = [f"%cd -q {d}", f"%run {fname}"]
            # exec_mngr represents the manager on spine-items side
            exec_mngr = KernelExecutionManager(logger, NATIVE_KERNEL_NAME, *cmds, group_id="SomeGroup")
            self.assertTrue(exec_mngr._kernel_manager.is_alive())
            exec_mngr = self.replace_client(exec_mngr)
            retval = exec_mngr.run_until_complete()  # Run commands
            self.assertEqual(0, retval)
            self.assertTrue(exec_mngr._kernel_manager.is_alive())
            connection_file = exec_mngr._kernel_manager.connection_file
            self.release_exec_mngr_resources(exec_mngr)
            self.assertEqual(1, _kernel_manager_factory.n_kernel_managers())
            _kernel_manager_factory.shutdown_kernel_manager(connection_file)
            self.assertEqual(0, _kernel_manager_factory.n_kernel_managers())
            (
                kernel_started_messages,
                stdin_messages,
                execution_started_messages,
                kernel_shutdown_messages,
            ) = self._collect_messages_per_type(logger)
            self.assertEqual(len(kernel_started_messages), 1)
            self.assertEqual(kernel_started_messages[0]["kernel_name"], NATIVE_KERNEL_NAME)
            self.assertEqual(len(stdin_messages), 2)
            expected_msg = {"type": "execution_started", "kernel_name": NATIVE_KERNEL_NAME}
            self.assertEqual([expected_msg], execution_started_messages)
            self.assertEqual(len(kernel_shutdown_messages), 0)

    def test_kernel_execution_manager_kill_completed(self):
        logger = MagicMock()
        logger.msg_kernel_execution.filter_id = ""
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as script_file:
            script_file.write('print("hello")')
            script_file.seek(0)
            d, fname = os.path.split(script_file.name)
            cmds = [f"%cd -q {d}", f"%run {fname}"]
            # exec_mngr represents the manager on spine-items side
            exec_mngr = KernelExecutionManager(logger, NATIVE_KERNEL_NAME, *cmds, kill_completed=True, group_id="a")
            self.assertTrue(exec_mngr._kernel_manager.is_alive())
            exec_mngr = self.replace_client(exec_mngr)
            retval = exec_mngr.run_until_complete()  # Run commands
            self.assertEqual(0, retval)
            self.assertFalse(exec_mngr._kernel_manager.is_alive())
            self.release_exec_mngr_resources(exec_mngr)
            self.assertEqual(0, _kernel_manager_factory.n_kernel_managers())
            (
                kernel_started_messages,
                stdin_messages,
                execution_started_messages,
                kernel_shutdown_messages,
            ) = self._collect_messages_per_type(logger)
            self.assertEqual(len(kernel_started_messages), 1)
            self.assertEqual(kernel_started_messages[0]["kernel_name"], NATIVE_KERNEL_NAME)
            self.assertEqual(len(stdin_messages), 2)
            expected_msg = {"type": "execution_started", "kernel_name": NATIVE_KERNEL_NAME}
            self.assertEqual([expected_msg], execution_started_messages)
            self.assertEqual(len(kernel_shutdown_messages), 1)
            self.assertEqual(kernel_shutdown_messages[0]["kernel_name"], NATIVE_KERNEL_NAME)

    def test_kernel_manager_sharing(self):
        logger1 = MagicMock()
        logger1.msg_kernel_execution.filter_id = ""
        logger2 = MagicMock()
        logger2.msg_kernel_execution.filter_id = ""
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as script_file1, tempfile.NamedTemporaryFile(
            "w+", encoding="utf-8"
        ) as script_file2:
            script_file1.write('print("hello")')
            script_file1.seek(0)
            script_file2.write('print("hello again")')
            script_file2.seek(0)
            d1, fname1 = os.path.split(script_file1.name)
            d2, fname2 = os.path.split(script_file2.name)
            exec_mngr1_cmds = [f"%cd -q {d1}", f"%run {fname1}"]
            exec_mngr2_cmds = [f"%cd -q {d2}", f"%run {fname2}"]
            kernel_name = NATIVE_KERNEL_NAME
            exec_mngr1 = KernelExecutionManager(logger1, kernel_name, *exec_mngr1_cmds, group_id="SomeGroup")
            exec_mngr1 = self.replace_client(exec_mngr1)
            retval1 = exec_mngr1.run_until_complete()  # Run commands
            self.assertEqual(0, retval1)
            exec_mngr2 = KernelExecutionManager(logger2, kernel_name, *exec_mngr2_cmds, group_id="SomeGroup")
            exec_mngr2 = self.replace_client(exec_mngr2)
            self.assertEqual(1, _kernel_manager_factory.n_kernel_managers())
            self.assertEqual(exec_mngr1._kernel_manager, exec_mngr2._kernel_manager)
            retval2 = exec_mngr2.run_until_complete()  # Run commands
            self.assertEqual(0, retval2)
            # Close
            self.release_exec_mngr_resources(exec_mngr1)
            self.release_exec_mngr_resources(exec_mngr2)
            _kernel_manager_factory.kill_kernel_managers()
            self.assertEqual(0, _kernel_manager_factory.n_kernel_managers())
            # Check emitted messages
            (
                kernel_started_messages,
                stdin_messages,
                execution_started_messages,
                kernel_shutdown_messages,
            ) = self._collect_messages_per_type(logger1)
            self.assertEqual(len(kernel_started_messages), 1)
            self.assertEqual(kernel_started_messages[0]["kernel_name"], NATIVE_KERNEL_NAME)
            self.assertEqual(len(stdin_messages), 2)
            expected_msg = {"type": "execution_started", "kernel_name": NATIVE_KERNEL_NAME}
            self.assertEqual([expected_msg], execution_started_messages)
            self.assertEqual(len(kernel_shutdown_messages), 0)
            (
                kernel_started_messages,
                stdin_messages,
                execution_started_messages,
                kernel_shutdown_messages,
            ) = self._collect_messages_per_type(logger2)
            self.assertEqual(len(kernel_started_messages), 1)
            self.assertEqual(kernel_started_messages[0]["kernel_name"], NATIVE_KERNEL_NAME)
            self.assertEqual(len(stdin_messages), 2)
            expected_msg = {"type": "execution_started", "kernel_name": NATIVE_KERNEL_NAME}
            self.assertEqual([expected_msg], execution_started_messages)
            self.assertEqual(len(kernel_shutdown_messages), 0)

    def test_two_kernel_managers(self):
        logger1 = MagicMock()
        logger1.msg_kernel_execution.filter_id = ""
        logger2 = MagicMock()
        logger2.msg_kernel_execution.filter_id = ""
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as script_file1, tempfile.NamedTemporaryFile(
            "w+", encoding="utf-8"
        ) as script_file2:
            script_file1.write('print("hello")')
            script_file1.seek(0)
            script_file2.write('print("hello again")')
            script_file2.seek(0)
            d1, fname1 = os.path.split(script_file1.name)
            d2, fname2 = os.path.split(script_file2.name)
            exec_mngr1_cmds = [f"%cd -q {d1}", f"%run {fname1}"]
            exec_mngr2_cmds = [f"%cd -q {d2}", f"%run {fname2}"]
            kernel_name = NATIVE_KERNEL_NAME
            exec_mngr1 = KernelExecutionManager(logger1, kernel_name, *exec_mngr1_cmds, group_id="SomeGroup")
            exec_mngr1 = self.replace_client(exec_mngr1)
            retval1 = exec_mngr1.run_until_complete()  # Run commands
            self.assertEqual(0, retval1)
            exec_mngr2 = KernelExecutionManager(logger2, kernel_name, *exec_mngr2_cmds, group_id="AnotherGroup")
            exec_mngr2 = self.replace_client(exec_mngr2)
            self.assertEqual(2, _kernel_manager_factory.n_kernel_managers())
            self.assertNotEqual(exec_mngr1._kernel_manager, exec_mngr2._kernel_manager)
            retval2 = exec_mngr2.run_until_complete()  # Run commands
            self.assertEqual(0, retval2)
            # Close
            self.release_exec_mngr_resources(exec_mngr1)
            self.release_exec_mngr_resources(exec_mngr2)
            _kernel_manager_factory.kill_kernel_managers()
            self.assertEqual(0, _kernel_manager_factory.n_kernel_managers())
            # Check emitted messages
            (
                kernel_started_messages,
                stdin_messages,
                execution_started_messages,
                kernel_shutdown_messages,
            ) = self._collect_messages_per_type(logger1)
            self.assertEqual(len(kernel_started_messages), 1)
            self.assertEqual(kernel_started_messages[0]["kernel_name"], NATIVE_KERNEL_NAME)
            self.assertEqual(len(stdin_messages), 2)
            expected_msg = {"type": "execution_started", "kernel_name": NATIVE_KERNEL_NAME}
            self.assertEqual([expected_msg], execution_started_messages)
            self.assertEqual(len(kernel_shutdown_messages), 0)
            (
                kernel_started_messages,
                stdin_messages,
                execution_started_messages,
                kernel_shutdown_messages,
            ) = self._collect_messages_per_type(logger2)
            self.assertEqual(len(kernel_started_messages), 1)
            self.assertEqual(kernel_started_messages[0]["kernel_name"], NATIVE_KERNEL_NAME)
            self.assertEqual(len(stdin_messages), 2)
            expected_msg = {"type": "execution_started", "kernel_name": NATIVE_KERNEL_NAME}
            self.assertEqual([expected_msg], execution_started_messages)
            self.assertEqual(len(kernel_shutdown_messages), 0)

    @staticmethod
    def replace_client(exec_mngr):
        """Reloads the connection file, and replaces the kernel client with a new one, just like
        we do on Toolbox side. Don't really understand why we need to do this, but I think
        it's because of the 'classic' race condition in jupyter-client < 7.0.
        This maybe fixed in a more recent version of jupyter-client."""
        # exec_mngr._kernel_client.stop_channels()
        # exec_mngr._kernel_client.context.term()  # ResourceWarning: Unclosed <zmq.Context() happens without this
        exec_mngr._kernel_manager.load_connection_file()
        kc = exec_mngr._kernel_manager.client()  # Make new client
        # kc.start_channels()
        exec_mngr._kernel_client = kc  # Replace the original client
        return exec_mngr

    def _collect_messages_per_type(self, logger):
        stdin_messages = []
        execution_started_messages = []
        kernel_started_messages = []
        kernel_shutdown_messages = []
        for i, call in enumerate(logger.msg_kernel_execution.emit.call_args_list):
            with self.subTest(call_number=i):
                self.assertEqual(len(call.kwargs), 0)
                self.assertEqual(len(call.args), 1)
            message = call.args[0]
            msg_type = message["type"]
            if msg_type == "stdin":
                stdin_messages.append(message)
            elif msg_type == "execution_started":
                execution_started_messages.append(message)
            elif msg_type == "stderr":
                # UserWarnings are harmless, everything else is suspicious
                self.assertIn("UserWarning", message["data"])
            elif msg_type == "kernel_started":
                kernel_started_messages.append(message)
            elif msg_type == "kernel_shutdown":
                kernel_shutdown_messages.append(message)
            else:
                self.fail(f"unexpected message type {msg_type}")
        return kernel_started_messages, stdin_messages, execution_started_messages, kernel_shutdown_messages
