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
Contains the KernelExecutionManager class and subclasses, and some convenience functions.

:authors: M. Marin (KTH)
:date:   12.10.2020
"""

import os
import sys
import subprocess
from jupyter_client.manager import KernelManager
from jupyter_client.kernelspec import NoSuchKernel
from ..utils.helpers import Singleton
from .execution_manager_base import ExecutionManagerBase
from spine_engine.execution_managers.conda_kernel_spec_manager import CondaKernelSpecManager


class _KernelManagerFactory(metaclass=Singleton):
    _kernel_managers = {}
    """Maps tuples (kernel name, group id) to associated KernelManager."""
    _key_by_connection_file = {}
    """Maps connection file string to tuple (kernel_name, group_id). Mostly for fast lookup in ``restart_kernel()``"""

    def _make_kernel_manager(self, kernel_name, group_id):
        """Creates a new kernel manager for given kernel and group id if none exists, and returns it.

        Args:
            kernel_name (str): the kernel
            group_id (str): item group that will execute using this kernel

        Returns:
            KernelManager
        """
        if group_id is None:
            # Execute in isolation
            return KernelManager(kernel_name=kernel_name)
        key = (kernel_name, group_id)
        if key not in self._kernel_managers:
            self._kernel_managers[key] = KernelManager(kernel_name=kernel_name)
        return self._kernel_managers[key]

    def new_kernel_manager(self, kernel_name, group_id, logger, extra_switches=None, environment="", **kwargs):
        """Creates a new kernel manager for given kernel and group id if none exists.
        Starts the kernel if not started, and returns it.

        Args:
            kernel_name (str): the kernel
            group_id (str): item group that will execute using this kernel
            logger (LoggerInterface): for logging
            extra_switches (list, optional): List of additional switches to julia or python.
                These come before the 'programfile'.
            environment (str): "conda" to launch a Conda kernel spec. "" for a regular kernel spec
            `**kwargs`: optional. Keyword arguments passed to ``KernelManager.start_kernel()``

        Returns:
            KernelManager
        """
        km = self._make_kernel_manager(kernel_name, group_id)
        conda_exe = kwargs.pop("conda_exe", "")
        if environment == "conda":
            km.kernel_spec_manager = CondaKernelSpecManager(conda_exe=conda_exe)
        msg_head = dict(kernel_name=kernel_name)
        if not km.is_alive():
            try:
                if not km.kernel_spec:  # TODO: Check if this is needed in addition to catching NoSuchKernel?
                    msg = dict(type="kernel_spec_not_found", **msg_head)
                    logger.msg_kernel_execution.emit(msg)
                    raise RuntimeError
            except NoSuchKernel:
                msg = dict(type="kernel_spec_not_found", **msg_head)
                logger.msg_kernel_execution.emit(msg)
                raise RuntimeError
            # Check that kernel spec executable is referring to a file that actually exists
            exe_path = km.kernel_spec.argv[0]
            if not os.path.exists(exe_path) and os.path.isabs(exe_path):
                msg_head["kernel_exe_path"] = exe_path
                msg = dict(type="kernel_spec_exe_not_found", **msg_head)
                logger.msg_kernel_execution.emit(msg)
                raise RuntimeError
            if extra_switches:
                # Insert switches right after the julia program
                km.kernel_spec.argv[1:1] = extra_switches
            km.start_kernel(**kwargs)
            self._key_by_connection_file[km.connection_file] = (kernel_name, group_id)
        msg = dict(type="kernel_started", connection_file=km.connection_file, **msg_head)
        logger.msg_kernel_execution.emit(msg)
        return km

    def get_kernel_manager(self, connection_file):
        """Returns a kernel manager for given connection file if any.

        Args:
            connection_file (str): path of connection file

        Returns:
            KernelManager or None
        """
        key = self._key_by_connection_file.get(connection_file)
        return self._kernel_managers.get(key)

    def pop_kernel_manager(self, connection_file):
        """Returns a kernel manager for given connection file if any.
        It also removes it from cache.

        Args:
            connection_file (str): path of connection file

        Returns:
            KernelManager or None
        """
        key = self._key_by_connection_file.pop(connection_file, None)
        return self._kernel_managers.pop(key, None)


_kernel_manager_factory = _KernelManagerFactory()


def get_kernel_manager(connection_file):
    return _kernel_manager_factory.get_kernel_manager(connection_file)


def pop_kernel_manager(connection_file):
    return _kernel_manager_factory.pop_kernel_manager(connection_file)


class KernelExecutionManager(ExecutionManagerBase):
    def __init__(
        self,
        logger,
        kernel_name,
        *commands,
        group_id=None,
        workdir=None,
        startup_timeout=60,
        extra_switches=None,
        environment="",
        **kwargs,
    ):
        """
        Args:
            logger (LoggerInterface)
            kernel_name (str): the kernel
            *commands: Commands to execute in the kernel
            group_id (str, optional): item group that will execute using this kernel
            workdir (str, optional): item group that will execute using this kernel
            startup_timeout (int, optional): How much to wait for the kernel, used in ``KernelClient.wait_for_ready()``
            extra_switches (list, optional): List of additional switches to launch julia.
                These come before the 'programfile'.
            environment (str): "conda" to launch a Conda kernel spec. "" for a regular kernel spec.
            **kwargs (optional): Keyword arguments passed to ``KernelManager.start_kernel()``
        """
        super().__init__(logger)
        self._msg_head = dict(kernel_name=kernel_name)
        self._commands = commands
        self._group_id = group_id
        self._workdir = workdir
        self._cmd_failed = False
        kwargs["stdout"] = open(os.devnull, 'w')
        kwargs["stderr"] = open(os.devnull, 'w')
        # Don't show console when frozen
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        self._kernel_manager = _kernel_manager_factory.new_kernel_manager(
            kernel_name,
            group_id,
            logger,
            cwd=self._workdir,
            extra_switches=extra_switches,
            environment=environment,
            **kwargs,
        )
        self._kernel_client = self._kernel_manager.client() if self._kernel_manager is not None else None
        self._startup_timeout = startup_timeout

    def run_until_complete(self):
        if self._kernel_client is None:
            return
        self._kernel_client.start_channels()
        run_succeeded = self._do_run()
        self._kernel_client.stop_channels()
        if self._cmd_failed or not run_succeeded:
            return -1
        return 0

    def _do_run(self):
        try:
            self._kernel_client.wait_for_ready(timeout=self._startup_timeout)
        except RuntimeError as e:
            msg = dict(type="execution_failed_to_start", error=str(e), **self._msg_head)
            self._logger.msg_kernel_execution.emit(msg)
            return False
        msg = dict(type="execution_started", **self._msg_head)
        self._logger.msg_kernel_execution.emit(msg)
        for cmd in self._commands:
            self._cmd_failed = False
            # 'reply' is an execute_reply msg coming from the shell (ROUTER/DEALER) channel, it's a response to
            # an execute_request msg
            reply = self._kernel_client.execute_interactive(cmd, output_hook=self._output_hook)
            st = reply["content"]["status"]
            if st != "ok":
                return False  # This happens when execute_request fails
        return True

    def _output_hook(self, msg):
        """Catches messages from the IOPUB (PUB/SUB) channel and handle case when message type is 'error'.
        'error' msg is a response to an execute_input msg."""
        if msg["header"]["msg_type"] == "error":
            self._cmd_failed = True

    def stop_execution(self):
        if self._kernel_manager is not None:
            self._kernel_manager.interrupt_kernel()
