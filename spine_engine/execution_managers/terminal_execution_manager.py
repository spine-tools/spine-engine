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

import sys
import socket
import socketserver
import signal
import uuid
import time
from subprocess import Popen, PIPE
from threading import Thread
from queue import Queue
from ..utils.helpers import Singleton
from .execution_manager_base import ExecutionManagerBase

if sys.platform == "win32":
    from subprocess import CREATE_NEW_PROCESS_GROUP


class _TerminalFactory(metaclass=Singleton):
    _terminals = {}
    """Maps tuples (*process args) to associated Popen."""

    @staticmethod
    def _do_make_terminal(term_args, logger, cwd):
        kwargs = dict(stdin=PIPE, stdout=PIPE, stderr=PIPE, cwd=cwd)
        if sys.platform == "win32":
            kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP
        try:
            p = Popen(term_args, **kwargs)
            msg = dict(type="terminal_started", pid=p.pid)
            logger.msg_terminal_execution.emit(msg)
            return p
        except OSError as e:
            msg = dict(type="terminal_failed_to_start", error=str(e), term_args=term_args)
            logger.msg_terminal_execution.emit(msg)
            return None

    def new_terminal(self, term_args, group_id, logger, cwd=None):
        """Creates a new terminal for given args and group id if none exists.

        Args:
            term_args (list): the terminal arguments
            group_id (str): item group that will execute using this terminal
            logger (LoggerInterface)

        Returns:
            Popen
        """
        if group_id is None:
            # Execute in isolation
            return self._do_make_terminal(term_args, logger, cwd)
        key = tuple(term_args + [group_id])
        if key not in self._terminals or self._terminals[key].return_code is not None:
            # return_code different than None means the terminal process has terminated
            self._terminals[key] = self._do_make_terminal(term_args, logger, cwd)
        return self._terminals[key]


_terminal_factory = _TerminalFactory()


class TerminalExecutionManagerBase(ExecutionManagerBase):
    def __init__(self, logger, term_args, commands, group_id=None, workdir=None):
        """Class constructor.

        Args:
            logger (LoggerInterface): a logger instance
            term_args (list): List of args to start the terminal
            commands (list): List of commands for the terminal to execute
        """
        super().__init__(logger)
        self._term_args = term_args
        self._commands = commands
        self._group_id = group_id
        self._workdir = workdir
        self._terminal = _terminal_factory.new_terminal(
            self._term_args, self._group_id, self._logger, cwd=self._workdir
        )
        self._done = False

    @staticmethod
    def _make_sentinel(host, port, secret):
        raise NotImplementedError()

    def run_until_complete(self):
        host = "127.0.0.1"
        with socketserver.TCPServer((host, 0), None) as s:
            port = s.server_address[1]
        secret = str(uuid.uuid4())
        queue = Queue()
        sentinel = self._make_sentinel(host, port, secret)
        Thread(target=self._enqueue_messages, args=(host, port, queue), daemon=True).start()
        Thread(target=self._log_stdout, args=(), daemon=True).start()
        Thread(target=self._log_stderr, args=(), daemon=True).start()
        for cmd in self._commands + [sentinel]:
            self._terminal.stdin.write(f"{cmd.strip()}\n".encode("UTF8"))
            try:
                self._terminal.stdin.flush()
            except BrokenPipeError:
                self._logger.msg_proc_error.emit(f"Error executing {cmd}. Restarting...")
                self._terminal.kill()
                self._terminal.wait()
                self._terminal = _terminal_factory.new_terminal(
                    self._term_args, self._group_id, self._logger, cwd=self._workdir
                )
                return self.run_until_complete()
        while True:
            msg = queue.get()
            queue.task_done()
            if msg == secret:
                break
        self._done = True
        return 0

    def stop_execution(self):
        if self._terminal is None:
            return
        Thread(target=self._do_stop_execution, args=(), daemon=True).start()

    def _do_stop_execution(self):
        sig = signal.CTRL_C_EVENT if sys.platform == "win32" else signal.SIGINT
        while not self._done:
            self._terminal.send_signal(sig)
            time.sleep(1)

    def _log_stdout(self):
        for line in iter(self._terminal.stdout.readline, b''):
            self._logger.msg_proc.emit(line.decode("UTF8", "replace").strip())

    def _log_stderr(self):
        for line in iter(self._terminal.stderr.readline, b''):
            self._logger.msg_proc_error.emit(line.decode("UTF8", "replace").strip())

    @staticmethod
    def _enqueue_messages(host, port, queue):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, port))
            s.listen()
            conn, _ = s.accept()
            with conn:
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    queue.put(data.decode("UTF8", "replace"))


class JuliaTerminalExecutionManager(TerminalExecutionManagerBase):
    @staticmethod
    def _make_sentinel(host, port, secret):
        return f'using Sockets; s = connect("{host}", {port}); write(s, "{secret}"); close(s);'
