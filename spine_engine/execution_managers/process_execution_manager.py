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

from subprocess import Popen, PIPE
from threading import Thread
from .execution_manager_base import ExecutionManagerBase


class ProcessExecutionManager(ExecutionManagerBase):
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
            self._process = Popen([self._program, *self._args], stdout=PIPE, stderr=PIPE, cwd=self._workdir)
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
            self._logger.msg_proc_error.emit(line.decode("UTF8", "replace").strip())
        stderr.close()