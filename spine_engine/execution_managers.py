######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
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


def _start_daemon_thread(target, *args):
    t = Thread(target=target, args=args)
    t.daemon = True  # thread dies with the program
    t.start()


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
        _start_daemon_thread(self._log_stdout, self._process.stdout)
        _start_daemon_thread(self._log_stderr, self._process.stderr)
        return self._process.wait()

    def stop_execution(self):
        if self._process is not None:
            self._process.terminate()

    def _log_stdout(self, stdout):
        for line in iter(stdout.readline, b''):
            self._logger.msg_proc.emit(line.decode("UTF8").strip())
        stdout.close()

    def _log_stderr(self, stderr):
        for line in iter(stderr.readline, b''):
            self._logger.msg_proc_error.emit(line.decode("UTF8").strip())
        stderr.close()


class _KernelManagerProvider(metaclass=Singleton):
    _kernel_managers = {}

    def new_kernel_manager(self, kernel_name, group_id):
        if group_id is None:
            # Execute in isolation
            return KernelManager(kernel_name=kernel_name)
        if (kernel_name, group_id) not in self._kernel_managers:
            self._kernel_managers[kernel_name, group_id] = KernelManager(kernel_name=kernel_name)
        return self._kernel_managers[kernel_name, group_id]


class KernelExecutionManager(ExecutionManagerBase):
    def __init__(self, logger, language, kernel_name, *commands, group_id=None, startup_timeout=60):
        super().__init__(logger)
        provider = _KernelManagerProvider()
        self._msg = dict(language=language, kernel_name=kernel_name)
        self._commands = commands
        self._group_id = group_id
        self._kernel_manager = provider.new_kernel_manager(kernel_name, group_id)
        self._startup_timeout = startup_timeout
        self._kernel_client = None

    def run_until_complete(self):
        if not self._kernel_manager.is_alive():
            # Start kernel and dispatch the event, so toolbox knows it needs to configure it's client
            if not self._kernel_manager.kernel_spec:
                msg = dict(type="kernel_spec_not_found", **self._msg)
                self._logger.msg_kernel_execution.emit(msg)
                return
            blackhole = open(os.devnull, 'w')
            self._kernel_manager.start_kernel(stdout=blackhole, stderr=blackhole)
        msg = dict(type="kernel_started", connection_file=self._kernel_manager.connection_file, **self._msg)
        self._logger.msg_kernel_execution.emit(msg)
        self._kernel_client = self._kernel_manager.client()
        self._kernel_client.start_channels()
        returncode = self._do_run()
        self._kernel_client.stop_channels()
        return returncode

    def _do_run(self):
        try:
            self._kernel_client.wait_for_ready(timeout=self._startup_timeout)
        except RuntimeError as e:
            msg = dict(type="execution_failed_to_start", error=str(e), **self._msg)
            self._logger.msg_kernel_execution.emit(msg)
            return
        msg = dict(type="execution_started", code=" ".join(self._commands), **self._msg)
        self._logger.msg_kernel_execution.emit(msg)
        for cmd in self._commands:
            reply = self._kernel_client.execute_interactive(cmd, output_hook=lambda msg: None)
            st = reply["content"]["status"]
            if st != "ok":
                return -1
        return 0

    def stop_execution(self):
        if self._kernel_client is not None:
            self._kernel_client.stop_channels()
