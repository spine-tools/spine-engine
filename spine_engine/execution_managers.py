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
Contains the ExeuctionManagerBase class and main subclasses.

:authors: M. Marin (KTH)
:date:   12.10.2020
"""

import os
from subprocess import Popen, PIPE
from threading import Thread
from jupyter_client.manager import KernelManager
from .utils.helpers import Singleton


class ExecutionManagerBase:
    """Base class for all tool instance execution managers."""

    def __init__(self, logger):
        """Class constructor.

        Args:
            logger (LoggerInterface): a logger instance
        """
        self._logger = logger

    def run_until_complete(self):
        """Runs until completion.

        Returns:
            int: return code
        """
        raise NotImplementedError()

    def stop_execution(self):
        """Stops execution gracefully."""
        raise NotImplementedError()


class StandardExecutionManager(ExecutionManagerBase):
    def __init__(self, logger, program, *args, workdir=None):
        """Class constructor.

        Args:
            logger (LoggerInterface): a logger instance
            program (str): Path to program to run in the subprocess (e.g. julia.exe)
            args (list): List of argument for the program (e.g. path to script file)
        """
        super().__init__(logger)
        self._process = None
        self._program = program
        self._args = args
        self._workdir = workdir

    def run_until_complete(self):
        try:
            self._process = Popen([self._program, *self._args], stdout=PIPE, stderr=PIPE, bufsize=1, cwd=self._workdir)
        except OSError as e:
            msg = dict(type="execution_failed_to_start", error=str(e), program=self._program)
            self._logger.msg_standard_execution.emit(msg)
            return
        msg = dict(type="execution_started", program=self._program, args=" ".join(self._args))
        self._logger.msg_standard_execution.emit(msg)
        Thread(target=self._log_stdout, args=(self._process.stdout,), daemon=True).start()
        Thread(target=self._log_stderr, args=(self._process.stderr,), daemon=True).start()
        return self._process.wait()

    def stop_execution(self):
        if self._process is not None:
            self._process.terminate()

    def _log_stdout(self, stdout):
        for line in iter(stdout.readline, b''):
            self._logger.msg_proc.emit(line.decode("UTF8", "replace").strip())
        stdout.close()

    def _log_stderr(self, stderr):
        for line in iter(stderr.readline, b''):
            self._logger.msg_proc_error.emit(line.decode("UTF8").strip())
        stderr.close()


class _KernelManagerFactory(metaclass=Singleton):
    _kernel_managers = {}
    """Maps tuples (kernel name, group id) to associated KernelManager."""
    _km_by_connection_file = {}
    """Maps connection file string to associated KernelManager. Mostly for fast lookup in ``restart_kernel()``"""

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
        return self._kernel_managers.setdefault((kernel_name, group_id), KernelManager(kernel_name=kernel_name))

    def new_kernel_manager(self, language, kernel_name, group_id, logger, **kwargs):
        """Creates a new kernel manager for given kernel and group id if none exists.
        Starts the kernel if not started, and returns it.

        Args:
            language (str): The underlying language, for logging purposes.
            kernel_name (str): the kernel
            group_id (str): item group that will execute using this kernel
            logger (LoggerInterface): for logging
            `**kwargs`: optional. Keyword arguments passed to ``KernelManager.start_kernel()``

        Returns:
            KernelManager
        """
        km = self._make_kernel_manager(kernel_name, group_id)
        if not km.is_alive():
            msg_head = dict(language=language, kernel_name=kernel_name)
            if not km.kernel_spec:
                msg = dict(type="kernel_spec_not_found", **msg_head)
                logger.msg_kernel_execution.emit(msg)
                return None
            blackhole = open(os.devnull, 'w')
            km.start_kernel(stdout=blackhole, stderr=blackhole, **kwargs)
            msg = dict(type="kernel_started", connection_file=km.connection_file, **msg_head)
            logger.msg_kernel_execution.emit(msg)
            self._km_by_connection_file[km.connection_file] = km
        return km

    def get_kernel_manager(self, connection_file):
        """Returns a kernel manager for given connection file if any.

        Args:
            connection_file (str): path of connection file

        Returns:
            KernelManager or None
        """
        return self._km_by_connection_file.get(connection_file)


_kernel_manager_factory = _KernelManagerFactory()


def get_kernel_manager(connection_file):
    return _kernel_manager_factory.get_kernel_manager(connection_file)


class KernelExecutionManager(ExecutionManagerBase):
    def __init__(self, logger, language, kernel_name, *commands, group_id=None, workdir=None, startup_timeout=60):
        super().__init__(logger)
        self._msg_head = dict(language=language, kernel_name=kernel_name)
        self._commands = commands
        self._group_id = group_id
        self._workdir = workdir
        self._kernel_manager = _kernel_manager_factory.new_kernel_manager(
            language, kernel_name, group_id, logger, cwd=self._workdir
        )
        self._kernel_client = self._kernel_manager.client() if self._kernel_manager is not None else None
        self._startup_timeout = startup_timeout

    def run_until_complete(self):
        if self._kernel_client is None:
            return
        self._kernel_client.start_channels()
        returncode = self._do_run()
        self._kernel_client.stop_channels()
        return returncode

    def _do_run(self):
        try:
            self._kernel_client.wait_for_ready(timeout=self._startup_timeout)
        except RuntimeError as e:
            msg = dict(type="execution_failed_to_start", error=str(e), **self._msg_head)
            self._logger.msg_kernel_execution.emit(msg)
            return
        msg = dict(type="execution_started", **self._msg_head)
        self._logger.msg_kernel_execution.emit(msg)
        for cmd in self._commands:
            reply = self._kernel_client.execute_interactive(cmd, output_hook=lambda msg: None)
            st = reply["content"]["status"]
            if st != "ok":
                return -1
        return 0

    def stop_execution(self):
        if self._kernel_manager is not None:
            self._kernel_manager.interrupt_kernel()
