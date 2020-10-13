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
import asyncio
from asyncio import create_subprocess_exec
from asyncio.subprocess import PIPE


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
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._run(workdir=workdir))

    async def _run(self, workdir=None):
        self.change_state(ExecutionState.STARTING)
        try:
            process = await create_subprocess_exec(self._program, *self._args, stdout=PIPE, stderr=PIPE, cwd=workdir)
        except OSError:
            self.change_state(ExecutionState.FAILED_TO_START)
            return
        self.change_state(ExecutionState.RUNNING)
        await asyncio.wait(
            [_read_stream(process.stdout, self.log_stdout), _read_stream(process.stderr, self.log_stderr)]
        )
        await process.wait()
        return process.returncode

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

    def log_stdout(self, stdout):
        self._logger.msg_proc.emit(stdout.decode("UTF8"))

    def log_stderr(self, stderr):
        self._logger.msg_proc_error.emit(stderr.decode("UTF8"))


async def _read_stream(stream, callback):
    while True:
        line = await stream.readline()
        if line:
            callback(line)
        else:
            break
