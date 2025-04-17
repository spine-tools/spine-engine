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

"""
Contains the KernelExecutionManager class and subclasses, and some convenience functions.

"""

import os
import subprocess
import sys
import uuid
from jupyter_client.kernelspec import NoSuchKernel
from jupyter_client.manager import KernelManager
from spine_engine.execution_managers.conda_kernel_spec_manager import CondaKernelSpecManager
from ..utils.helpers import Singleton
from .execution_manager_base import ExecutionManagerBase


class GroupedKernelManager(KernelManager):
    """Kernel Manager that supports group ID's."""

    def __init__(self, *args, **kwargs):
        group_id = kwargs.pop("group_id", "")
        super().__init__(*args, **kwargs)
        self._group_id = group_id
        self._is_busy = False

    def group_id(self):
        """Returns the group ID of this kernel manager."""
        return self._group_id

    def set_busy(self, f):
        """Sets km busy. This is set according to the
        status messages received from the IOPUB channel."""
        self._is_busy = f

    def is_busy(self):
        """Returns whether km is busy or not."""
        return self._is_busy


class _KernelManagerFactory(metaclass=Singleton):
    _kernel_managers = {}
    """Maps filter_id (str) to associated KernelManager"""
    _key_by_connection_file = {}
    """Maps connection file path to filter_id (str)"""

    def _make_kernel_manager(self, kernel_name, group_id, server_ip, filter_id):
        """Creates a new kernel manager if necessary or returns an existing one.

        Args:
            kernel_name (str): The kernel
            group_id (str): Item group that will execute using this kernel
            server_ip (str): Engine Server IP address. '127.0.0.1' when execution happens locally
            filter_id (str): Filter Id

        Returns:
            GroupedKernelManager
        """
        if not filter_id == "":
            group_id = filter_id  # Ignore group ID in case filter ID exists
        for k in self._kernel_managers:
            # Reuse kernel manager if using same group id and kernel and it's idle
            km = self._kernel_managers[k]
            if km.group_id() == group_id and km.kernel_name == kernel_name:
                if not km.is_busy():
                    return km
        key = uuid.uuid4().hex
        # Spawn a new kernel manager
        km = self._kernel_managers[key] = GroupedKernelManager(kernel_name=kernel_name, ip=server_ip, group_id=group_id)
        return km

    def new_kernel_manager(self, kernel_name, group_id, logger, extra_switches=None, environment="", **kwargs):
        """Creates a new kernel manager for given kernel and group id if none exists.
        Starts the kernel if not started, and returns it.

        Args:
            kernel_name (str): The kernel
            group_id (str): Item group that will execute using this kernel
            logger (LoggerInterface): For logging
            extra_switches (list, optional): List of additional switches for the kernel (i.e. Julia or Python)
            environment (str): "conda" to launch a Conda kernel spec. "" for a regular kernel spec
            `**kwargs`: optional. Keyword arguments passed to ``KernelManager.start_kernel()``

        Returns:
            KernelManager
        """
        server_ip = kwargs.pop("server_ip", "")
        filter_id = logger.msg_kernel_execution.filter_id
        km = self._make_kernel_manager(kernel_name, group_id, server_ip, filter_id)
        conda_exe = kwargs.pop("conda_exe", "")
        if environment == "conda":
            if not os.path.exists(conda_exe):
                logger.msg_kernel_execution.emit(msg=dict(type="conda_not_found"))
                self._kernel_managers.pop(self.get_kernel_manager_key(km))
                return None
            km.kernel_spec_manager = CondaKernelSpecManager(conda_exe=conda_exe)
        msg = dict(kernel_name=kernel_name)
        if not km.is_alive():
            try:
                if not km.kernel_spec:
                    # Happens when a conda kernel spec with the requested name cannot be dynamically created
                    # i.e. the conda environment does not exist
                    msg["type"] = "conda_kernel_spec_not_found"
                    logger.msg_kernel_execution.emit(msg)
                    self._kernel_managers.pop(self.get_kernel_manager_key(km))  # Delete failed kernel manager
                    return None
            except NoSuchKernel:
                msg["type"] = "kernel_spec_not_found"
                logger.msg_kernel_execution.emit(msg)
                self._kernel_managers.pop(self.get_kernel_manager_key(km))
                return None
            # Check that kernel spec executable is referring to a file that actually exists
            exe_path = km.kernel_spec.argv[0]
            if not os.path.exists(exe_path) and os.path.isabs(exe_path):
                msg["type"] = "kernel_spec_exe_not_found"
                msg["kernel_exe_path"] = exe_path
                logger.msg_kernel_execution.emit(msg)
                self._kernel_managers.pop(self.get_kernel_manager_key(km))
                return None
            if extra_switches:
                # Insert switches right after the julia program
                km.kernel_spec.argv[1:1] = extra_switches
            km.start_kernel(**kwargs)
            key = self.get_kernel_manager_key(km)
            if not key:
                return None  # Logic error
            self._key_by_connection_file[km.connection_file] = key
        msg["type"] = "kernel_started"
        msg["connection_file"] = km.connection_file
        logger.msg_kernel_execution.emit(msg)
        return km

    def get_kernel_manager_key(self, km):
        """Returns the key of the given kernel manager stored in this factory.

        Args:
            km (GroupedKernelManager): Kernel manager

        Returns:
            str: Kernel Manager's 32 character key
        """
        for key, kernman in self._kernel_managers.items():
            if kernman == km:
                return key
        return None

    def get_kernel_manager(self, connection_file):
        """Returns a kernel manager for given connection file if any.

        Args:
            connection_file (str): Path of connection file

        Returns:
            GroupedKernelManager or None
        """
        key = self._key_by_connection_file.get(connection_file, None)
        return self._kernel_managers.get(key, None)

    def pop_kernel_manager(self, connection_file):
        """Returns a kernel manager for given connection file if any.
        It also removes it from cache.

        Args:
            connection_file (str): Path of connection file

        Returns:
            GroupedKernelManager or None
        """
        key = self._key_by_connection_file.pop(connection_file, None)
        return self._kernel_managers.pop(key, None)

    def shutdown_kernel_manager(self, connection_file):
        """Pops a kernel manager from factory and shuts it down.

        Args:
            connection_file (str): Path of connection file

        Returns:
            bool: True if operation succeeded, False otherwise
        """
        km = self.pop_kernel_manager(connection_file)
        if not km:
            return False
        if km.is_alive():
            km.shutdown_kernel(now=True)
            return True
        return False

    def restart_kernel_manager(self, connection_file):
        """Restarts kernel manager.

        Args:
            connection_file (str): Path of connection file

        Returns:
            bool: True if operation succeeded, False otherwise
        """
        km = self.get_kernel_manager(connection_file)
        if not km:
            return False
        if km.is_alive():
            km.restart_kernel(now=True)
            return True
        return False

    def kill_kernel_managers(self):
        """Shuts down all kernel managers stored in the factory."""
        while True:
            try:
                key, km = self._kernel_managers.popitem()
                if km.is_alive():
                    km.shutdown_kernel(now=True)
            except KeyError:
                break
        self._key_by_connection_file.clear()

    def n_kernel_managers(self):
        """Returns the number of open kernel managers stored in the factory."""
        return len(self._kernel_managers)


_kernel_manager_factory = _KernelManagerFactory()


def get_kernel_manager(connection_file):
    return _kernel_manager_factory.get_kernel_manager(connection_file)


def pop_kernel_manager(connection_file):
    return _kernel_manager_factory.pop_kernel_manager(connection_file)


def n_kernel_managers():
    return _kernel_manager_factory.n_kernel_managers()


def kill_all_kernel_managers():
    _kernel_manager_factory.kill_kernel_managers()


def shutdown_kernel_manager(connection_file):
    return _kernel_manager_factory.shutdown_kernel_manager(connection_file)


def restart_kernel_manager(connection_file):
    return _kernel_manager_factory.restart_kernel_manager(connection_file)


class KernelExecutionManager(ExecutionManagerBase):
    def __init__(
        self,
        logger,
        kernel_name,
        commands,
        kill_completed=False,
        group_id=None,
        startup_timeout=60,
        environment="",
        extra_switches=None,
        **kwargs,
    ):
        """
        Args:
            logger (LoggerInterface): Logger instance
            kernel_name (str): Kernel name
            commands (list): Commands to execute in the kernel
            kill_completed (bool): Whether to kill completed persistent processes
            group_id (str, optional): Item group that will execute using this kernel
            startup_timeout (int, optional): How much to wait for the kernel, used in ``KernelClient.wait_for_ready()``
            environment (str): "conda" to launch a Conda kernel spec. "" for a regular kernel spec.
            extra_switches (list, optional): List of additional switches for the kernel (i.e. Julia or Python)
            **kwargs (optional): Keyword arguments passed to ``KernelManager.start_kernel()``
        """
        super().__init__(logger)
        self._msg_head = dict(kernel_name=kernel_name)
        self._commands = commands
        self._cmd_failed = False
        self.std_out = kwargs["stdout"] = open(os.devnull, "w")
        self.std_err = kwargs["stderr"] = open(os.devnull, "w")
        # Don't show console when frozen
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        self._kernel_manager = _kernel_manager_factory.new_kernel_manager(
            kernel_name, group_id, logger, extra_switches=extra_switches, environment=environment, **kwargs
        )
        self._kernel_client = self._kernel_manager.client() if self._kernel_manager is not None else None
        self._startup_timeout = startup_timeout
        self._kill_completed = kill_completed

    def run_until_complete(self):
        if self._kernel_client is None:
            return -1
        self._kernel_client.start_channels()
        run_succeeded = self._do_run()
        self._kernel_client.stop_channels()
        if self._kill_completed:
            conn_file = self._kernel_manager.connection_file
            shutdown_kernel_manager(conn_file)
            self._logger.msg_kernel_execution.emit(dict(type="kernel_shutdown", **self._msg_head))
        if self._cmd_failed or not run_succeeded:
            return -1
        return 0

    def _do_run(self):
        try:
            self._kernel_client.wait_for_ready(timeout=self._startup_timeout)
        except RuntimeError as e:
            msg = dict(type="execution_failed_to_start", error=str(e), **self._msg_head)
            self._logger.msg_kernel_execution.emit(msg)
            self._kernel_client.stop_channels()
            self._kernel_manager.shutdown_kernel(now=True)
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
        """Catches messages from the IOPUB (PUB/SUB) channel and
        handles 'error' and 'status message tyoe cases. 'error'
        msg is a response to an execute_input msg."""
        if msg["header"]["msg_type"] == "error":
            self._cmd_failed = True
        elif msg["header"]["msg_type"] == "status":
            # Set kernel manager busy if execution is starting or in progress
            exec_state = msg["content"]["execution_state"]
            if exec_state == "busy" or exec_state == "starting":
                self._kernel_manager.set_busy(True)
            else:  # exec_state == 'idle'
                self._kernel_manager.set_busy(False)
        elif msg["header"]["msg_type"] == "execute_input":
            execution_count, code = msg["content"]["execution_count"], msg["content"]["code"]
            self._logger.msg_kernel_execution.emit({"type": "stdin", "data": f"In [{execution_count}]: {code}"})
        elif msg["header"]["msg_type"] == "stream":
            self._logger.msg_kernel_execution.emit({"type": msg["content"]["name"], "data": msg["content"]["text"]})

    def stop_execution(self):
        if self._kernel_manager is not None:
            self._kernel_manager.interrupt_kernel()
            if self._kill_completed:
                conn_file = self._kernel_manager.connection_file
                shutdown_kernel_manager(conn_file)
                self._logger.msg_kernel_execution.emit(dict(type="kernel_shutdown", **self._msg_head))
