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

from enum import auto, Enum
from subprocess import Popen, PIPE
from threading import Thread


class ExecutionManagerBase:
    """Base class for all tool instance execution managers."""

    def __init__(self, logger):
        """Class constructor.

        Args:
            logger (LoggerInterface): a logger instance
        """
        self._logger = logger

    def run_until_complete(self, workdir=None):
        """Runs until completion.

        Returns:
            int: return code
        """
        raise NotImplementedError()


class ExecutionState(Enum):
    STARTING = auto()
    RUNNING = auto()
    FAILED_TO_START = auto()


def _start_daemon_thread(target, *args):
    t = Thread(target=target, args=args)
    t.daemon = True  # thread dies with the program
    t.start()


class StandardExecutionManager(ExecutionManagerBase):
    def __init__(self, logger, program, *args):
        """Class constructor.

        Args:
            logger (LoggerInterface): a logger instance
            program (str): Path to program to run in the subprocess (e.g. julia.exe)
            args (list): List of argument for the program (e.g. path to script file)
        """
        super().__init__(logger)
        self._program = program
        self._args = args
        self.process_failed_to_start = False

    def run_until_complete(self, workdir=None):
        self.change_state(ExecutionState.STARTING)
        try:
            p = Popen([self._program, *self._args], stdout=PIPE, stderr=PIPE, bufsize=1, cwd=workdir)
        except OSError:
            self.change_state(ExecutionState.FAILED_TO_START)
            return
        self.change_state(ExecutionState.RUNNING)
        _start_daemon_thread(self.log_stdout, p.stdout)
        _start_daemon_thread(self.log_stderr, p.stderr)
        return p.wait()

    def log_stdout(self, stdout):
        for line in iter(stdout.readline, b''):
            self._logger.msg_proc.emit(line.decode("UTF8"))
        stdout.close()

    def log_stderr(self, stderr):
        for line in iter(stderr.readline, b''):
            self._logger.msg_proc_error.emit(line.decode("UTF8"))
        stderr.close()

    def change_state(self, state):
        if state == ExecutionState.STARTING:
            self._logger.msg.emit("\tStarting program <b>{0}</b>".format(self._program))
            arg_str = " ".join(self._args)
            self._logger.msg.emit("\tArguments: <b>{0}</b>".format(arg_str))
        elif state == ExecutionState.FAILED_TO_START:
            self.process_failed_to_start = True
            self._logger.msg_error.emit("Process failed to start")
        elif state == ExecutionState.RUNNING:
            self._logger.msg_warning.emit("\tExecution is in progress. See Process Log for messages " "(stdout&stderr)")
