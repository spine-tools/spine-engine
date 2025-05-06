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

""" Contains the ProcessExecutionManager class. """

import subprocess
import sys
from threading import Thread
from ..utils.execution_resources import one_shot_process_semaphore
from .execution_manager_base import ExecutionManagerBase


class ProcessExecutionManager(ExecutionManagerBase):
    def __init__(self, logger, program, args, workdir=None):
        """Class constructor.

        Args:
            logger (LoggerInterface): Logger instance
            program (str): Path to program to run in the subprocess (e.g. 'path/to/julia')
            args (list): List of arguments for the program (e.g. path to script file)
            workdir (str): Work directory for the subprocess.Popen process
        """
        super().__init__(logger)
        self._process = None
        self._program = program
        self._args = args
        self._workdir = workdir
        self._stopped = False

    def run_until_complete(self):
        self._stopped = False
        cf = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0  # Don't show console when frozen
        with one_shot_process_semaphore:
            if self._stopped:
                return 0
            try:
                self._process = subprocess.Popen(
                    [self._program] + self._args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=self._workdir,
                    creationflags=cf,
                )
            except OSError as e:
                msg = dict(type="execution_failed_to_start", error=str(e), program=self._program)
                self._logger.msg_standard_execution.emit(msg)
                return 1
            msg = dict(type="execution_started", program=self._program, args=" ".join(self._args))
            self._logger.msg_standard_execution.emit(msg)
            running = "# Running" + " ".join([self._program] + self._args)
            self._logger.msg_standard_execution.emit({"type": "stdin", "data": running})
            Thread(target=self._log_stdout, args=(self._process.stdout,), daemon=True).start()
            Thread(target=self._log_stderr, args=(self._process.stderr,), daemon=True).start()
            return self._process.wait()

    def stop_execution(self):
        self._stopped = True
        if self._process is not None:
            self._process.terminate()

    def _log_stdout(self, stdout):
        for line in iter(stdout.readline, b""):
            line = line.decode("UTF8", "replace").strip()
            self._logger.msg_proc.emit(line)
            self._logger.msg_standard_execution.emit({"type": "stdout", "data": line})
        stdout.close()

    def _log_stderr(self, stderr):
        for line in iter(stderr.readline, b""):
            line = line.decode("UTF8", "replace").strip()
            self._logger.msg_proc_error.emit(line)
            self._logger.msg_standard_execution.emit({"type": "stderr", "data": line})
        stderr.close()
