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
"""Unit tests for ``kernel_execution_manager`` module."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock
from jupyter_client.kernelspec import NATIVE_KERNEL_NAME  # =='python3'
from spine_engine.execution_managers.kernel_execution_manager import KernelExecutionManager, _kernel_manager_factory


class TestKernelExecutionManager(unittest.TestCase):
    def setUp(self):
        self._temp_dir = TemporaryDirectory()

    def tearDown(self):
        self._temp_dir.cleanup()

    @staticmethod
    def release_exec_mngr_resources(mngr):
        """Frees resources after exec_mngr has been used. Consider putting
        this to KernelExecutionManager.close() or something."""
        if not mngr._kernel_client.context.closed:
            mngr._kernel_client.context.term()  # ResourceWarning: Unclosed <zmq.Context() happens without this
        mngr.std_out.close()  # Prevents ResourceWarning: unclosed file <_io.TextIOWrapper name='nul' ...
        mngr.std_err.close()  # Prevents ResourceWarning: unclosed file <_io.TextIOWrapper name='nul' ...
        return mngr

    def test_kernel_execution_manager(self):
        logger = MagicMock()
        logger.msg_kernel_execution.filter_id = ""
        f_name = "hello.py"
        file_path = Path(self._temp_dir.name, f_name)
        with open(file_path, "w") as fp:
            fp.writelines(["print('hello')\n"])
        cmds = [f"%cd -q {self._temp_dir.name}", f"%run {f_name}"]
        # exec_mngr represents the manager on spine-items side
        exec_mngr = KernelExecutionManager(logger, NATIVE_KERNEL_NAME, cmds, group_id="SomeGroup")
        self.assertTrue(exec_mngr._kernel_manager.is_alive())
        exec_mngr = self.replace_client(exec_mngr)
        retval = exec_mngr.run_until_complete()  # Run commands
        self.assertEqual(0, retval)
        self.assertTrue(exec_mngr._kernel_manager.is_alive())
        connection_file = exec_mngr._kernel_manager.connection_file
        self.release_exec_mngr_resources(exec_mngr)
        del exec_mngr
        self.assertEqual(1, _kernel_manager_factory.n_kernel_managers())
        _kernel_manager_factory.shutdown_kernel_manager(connection_file)
        self.assertEqual(0, _kernel_manager_factory.n_kernel_managers())
        message_emits = logger.msg_kernel_execution.emit.call_args_list
        expected_msg = {"type": "execution_started", "kernel_name": NATIVE_KERNEL_NAME}
        last_expected_msg = {"type": "stdout", "data": "hello\n"}
        # NOTE: In GitHub unittest runner on Ubuntu Python 3.9+ there is an extra warning message in message_emits
        # This makes the number of message_emits inconsistent between Pythons so
        # we don't do test self.assertEqual(5, len(message_emits))
        # {'type': 'stderr', 'data': "/opt/hostedtoolcache/Python/3.9.18/x64/lib/python3.9/site-packages/IPython/core
        # /magics/osm.py:417: UserWarning: using dhist requires you to install the `pickleshare` library.\n
        # self.shell.db['dhist'] = compress_dhist(dhist)[-100:]\n"}
        self.assertEqual(expected_msg, message_emits[1][0][0])
        self.assertEqual(last_expected_msg, message_emits[-1][0][0])

    def test_kernel_execution_manager_kill_completed(self):
        logger = MagicMock()
        logger.msg_kernel_execution.filter_id = ""
        f_name = "hello.py"
        file_path = Path(self._temp_dir.name, f_name)
        with open(file_path, "w") as fp:
            fp.writelines(["print('hello')\n"])
        cmds = [f"%cd -q {self._temp_dir.name}", f"%run {f_name}"]
        # exec_mngr represents the manager on spine-items side
        exec_mngr = KernelExecutionManager(logger, NATIVE_KERNEL_NAME, cmds, kill_completed=True, group_id="a")
        self.assertTrue(exec_mngr._kernel_manager.is_alive())
        exec_mngr = self.replace_client(exec_mngr)
        retval = exec_mngr.run_until_complete()  # Run commands
        self.assertEqual(0, retval)
        self.assertFalse(exec_mngr._kernel_manager.is_alive())
        self.release_exec_mngr_resources(exec_mngr)
        self.assertEqual(0, _kernel_manager_factory.n_kernel_managers())
        message_emits = logger.msg_kernel_execution.emit.call_args_list
        expected_msg = {"type": "execution_started", "kernel_name": NATIVE_KERNEL_NAME}
        last_expected_msg = {"type": "kernel_shutdown", "kernel_name": "python3"}
        self.assertEqual(expected_msg, message_emits[1][0][0])
        self.assertEqual(last_expected_msg, message_emits[-1][0][0])

    def test_kernel_manager_sharing(self):
        logger1 = MagicMock()
        logger1.msg_kernel_execution.filter_id = ""
        logger2 = MagicMock()
        logger2.msg_kernel_execution.filter_id = ""
        f_name1 = "hello.py"
        f_name2 = "hello_again.py"
        file_path1 = Path(self._temp_dir.name, f_name1)
        file_path2 = Path(self._temp_dir.name, f_name2)
        with open(file_path1, "w") as fp1:
            fp1.writelines(["print('hello')\n"])
        with open(file_path2, "w") as fp2:
            fp2.writelines(["print('hello again')\n"])
        exec_mngr1_cmds = [f"%cd -q {self._temp_dir.name}", f"%run {f_name1}"]
        exec_mngr2_cmds = [f"%cd -q {self._temp_dir.name}", f"%run {f_name2}"]
        kernel_name = NATIVE_KERNEL_NAME
        exec_mngr1 = KernelExecutionManager(logger1, kernel_name, exec_mngr1_cmds, group_id="SomeGroup")
        exec_mngr1 = self.replace_client(exec_mngr1)
        retval1 = exec_mngr1.run_until_complete()  # Run commands
        self.assertEqual(0, retval1)
        exec_mngr2 = KernelExecutionManager(logger2, kernel_name, exec_mngr2_cmds, group_id="SomeGroup")
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
        logger1_message_emits = logger1.msg_kernel_execution.emit.call_args_list
        logger2_message_emits = logger2.msg_kernel_execution.emit.call_args_list
        expected_msg = {"type": "execution_started", "kernel_name": NATIVE_KERNEL_NAME}
        last_logger1_expected_msg = {"type": "stdout", "data": "hello\n"}
        last_logger2_expected_msg = {"type": "stdout", "data": "hello again\n"}
        self.assertEqual(expected_msg, logger1_message_emits[1][0][0])
        self.assertEqual(last_logger1_expected_msg, logger1_message_emits[-1][0][0])
        self.assertEqual(expected_msg, logger2_message_emits[1][0][0])
        self.assertEqual(last_logger2_expected_msg, logger2_message_emits[-1][0][0])

    def test_two_kernel_managers(self):
        logger1 = MagicMock()
        logger1.msg_kernel_execution.filter_id = ""
        logger2 = MagicMock()
        logger2.msg_kernel_execution.filter_id = ""
        f_name1 = "hello.py"
        f_name2 = "hello_again.py"
        file_path1 = Path(self._temp_dir.name, f_name1)
        file_path2 = Path(self._temp_dir.name, f_name2)
        with open(file_path1, "w") as fp1:
            fp1.writelines(["print('hello')\n"])
        with open(file_path2, "w") as fp2:
            fp2.writelines(["print('hello again')\n"])
        exec_mngr1_cmds = [f"%cd -q {self._temp_dir.name}", f"%run {f_name1}"]
        exec_mngr2_cmds = [f"%cd -q {self._temp_dir.name}", f"%run {f_name2}"]
        kernel_name = NATIVE_KERNEL_NAME
        exec_mngr1 = KernelExecutionManager(logger1, kernel_name, exec_mngr1_cmds, group_id="SomeGroup")
        exec_mngr1 = self.replace_client(exec_mngr1)
        retval1 = exec_mngr1.run_until_complete()  # Run commands
        self.assertEqual(0, retval1)
        exec_mngr2 = KernelExecutionManager(logger2, kernel_name, exec_mngr2_cmds, group_id="AnotherGroup")
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
        logger1_message_emits = logger1.msg_kernel_execution.emit.call_args_list
        logger2_message_emits = logger2.msg_kernel_execution.emit.call_args_list
        expected_msg = {"type": "execution_started", "kernel_name": NATIVE_KERNEL_NAME}
        last_logger1_expected_msg = {"type": "stdout", "data": "hello\n"}
        last_logger2_expected_msg = {"type": "stdout", "data": "hello again\n"}
        self.assertEqual(expected_msg, logger1_message_emits[1][0][0])
        self.assertEqual(last_logger1_expected_msg, logger1_message_emits[-1][0][0])
        self.assertEqual(expected_msg, logger2_message_emits[1][0][0])
        self.assertEqual(last_logger2_expected_msg, logger2_message_emits[-1][0][0])

    @staticmethod
    def replace_client(exec_mngr):
        """Reloads the connection file, and replaces the kernel client with a new one, just like
        we do on Toolbox side. Don't really understand why we need to do this, but I think
        it's because of the 'classic' race condition in jupyter-client < 7.0.
        This may be fixed in a more recent version of jupyter-client."""
        exec_mngr._kernel_manager.load_connection_file()
        kc = exec_mngr._kernel_manager.client()  # Make new client
        exec_mngr._kernel_client = kc  # Replace the original client
        return exec_mngr
