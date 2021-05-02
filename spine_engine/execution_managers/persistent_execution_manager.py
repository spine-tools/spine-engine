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
import os
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


class _PersistentFactory(metaclass=Singleton):
    _persistents = {}
    """Maps tuples (*process args) to associated Popen."""

    @staticmethod
    def _do_make_persistent(args, cwd):
        kwargs = dict(stdin=PIPE, stdout=PIPE, stderr=PIPE, cwd=cwd)
        if sys.platform == "win32":
            kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP
        return Popen(args, **kwargs)

    def new_persistent(self, args, group_id, cwd=None):
        """Creates a new persistent for given args and group id if none exists.

        Args:
            args (list): the persistent arguments
            group_id (str): item group that will execute using this persistent
            logger (LoggerInterface)

        Returns:
            Popen
        """
        if group_id is None:
            # Execute in isolation
            return self._do_make_persistent(args, cwd)
        key = tuple(args + [group_id])
        if key not in self._persistents or self._persistents[key].returncode is not None:
            # returncode different than None means the persistent process has terminated
            self._persistents[key] = self._do_make_persistent(args, cwd)
        return self._persistents[key]


_persistent_factory = _PersistentFactory()


class PersistentExecutionManagerBase(ExecutionManagerBase):
    def __init__(self, logger, args, commands, group_id=None, workdir=None):
        """Class constructor.

        Args:
            logger (LoggerInterface): a logger instance
            args (list): List of args to start the persistent process
            commands (list): List of commands to execute in the persistent process
        """
        super().__init__(logger)
        self._args = args
        self._commands = commands
        self._group_id = group_id
        self._workdir = workdir
        self._done = False
        try:
            self._persistent = _persistent_factory.new_persistent(self._args, self._group_id, cwd=self._workdir)
            msg = dict(
                type="persistent_started", name=" ".join(self._args), lexer_name=self.lexer_name, prompt=self.prompt
            )
        except OSError as err:
            msg = dict(type="persistent_failed_to_start", error=str(err), args=args)
        self._logger.msg_persistent_execution.emit(msg)

    @staticmethod
    def _make_sentinel(host, port, secret):
        raise NotImplementedError()

    @property
    def lexer_name(self):
        raise NotImplementedError()

    @property
    def prompt(self):
        raise NotImplementedError()

    def run_until_complete(self):
        # FIXME: Move these to the factory class?
        Thread(target=self._log_stdout, args=(), daemon=True).start()
        Thread(target=self._log_stderr, args=(), daemon=True).start()
        for cmd in self._commands:
            self._logger.msg_persistent_execution.emit(dict(type="stdin", data=cmd.strip()))
            if not self._issue_command(cmd) or not self._wait_for_command_to_finish():
                self._logger.msg_proc_error.emit(f"Error executing {cmd}. Restarting...")
                self._replace_persistent()
                return self.run_until_complete()
        self._done = True
        return 0

    def _issue_command(self, cmd):
        self._persistent.stdin.write(f"{cmd.strip()}{os.linesep}".encode("UTF8"))
        try:
            self._persistent.stdin.flush()
            return True
        except BrokenPipeError:
            return False

    def _wait_for_command_to_finish(self):
        host = "127.0.0.1"
        with socketserver.TCPServer((host, 0), None) as s:
            port = s.server_address[1]
        queue = Queue()
        thread = Thread(target=self._listen_and_enqueue, args=(host, port, queue))
        thread.start()
        secret = str(uuid.uuid4())
        sentinel = self._make_sentinel(host, port, secret)
        if not self._issue_command(sentinel):
            return False
        while True:
            msg = queue.get()
            queue.task_done()
            if msg == secret:
                break
        thread.join()
        return True

    @staticmethod
    def _listen_and_enqueue(host, port, queue):
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

    def _replace_persistent(self):
        self._persistent.kill()
        self._persistent.wait()
        self._persistent = _persistent_factory.new_persistent(
            self._args, self._group_id, self._logger, cwd=self._workdir
        )

    def stop_execution(self):
        if self._persistent is None:
            return
        Thread(target=self._do_stop_execution, args=(), daemon=True).start()

    def _do_stop_execution(self):
        sig = signal.CTRL_C_EVENT if sys.platform == "win32" else signal.SIGINT
        while not self._done:
            self._persistent.send_signal(sig)
            time.sleep(1)

    def _log_stdout(self):
        for line in iter(self._persistent.stdout.readline, b''):
            data = line.decode("UTF8", "replace").strip()
            self._logger.msg_persistent_execution.emit(dict(type="stdout", data=data))

    def _log_stderr(self):
        for line in iter(self._persistent.stderr.readline, b''):
            data = line.decode("UTF8", "replace").strip()
            self._logger.msg_persistent_execution.emit(dict(type="stderr", data=data))


class JuliaPersistentExecutionManager(PersistentExecutionManagerBase):
    @staticmethod
    def _make_sentinel(host, port, secret):
        return f'using Sockets; let s = connect("{host}", {port}); write(s, "{secret}"); close(s) end;'

    @property
    def lexer_name(self):
        return "julia"

    @property
    def prompt(self):
        return '<br><span style="color:green; font-weight: bold">julia></span> '
