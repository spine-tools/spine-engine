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
Contains PersistentManagerBase, PersistentExecutionManagerBase classes and subclasses,
as well as some convenience functions.

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
import textwrap
from subprocess import Popen, PIPE
from threading import Thread
from queue import Queue
from ..utils.helpers import Singleton
from .execution_manager_base import ExecutionManagerBase

if sys.platform == "win32":
    from subprocess import CREATE_NEW_PROCESS_GROUP


class PersistentManagerBase:
    _COMMAND_FINISHED = object()

    def __init__(self, args, cwd=None):
        """
        Args:
            args (list): the arguments to launch the persistent process
            cwd (str, optional): the directory where to start the process
        """
        self._args = args
        self._kwargs = dict(stdin=PIPE, stdout=PIPE, stderr=PIPE, cwd=cwd)
        if sys.platform == "win32":
            self._kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP
        self._persistent = Popen(self._args, **self._kwargs)
        self._idle = True
        self._queue = Queue()
        self.command_successful = False
        Thread(target=self._log_stdout, daemon=True).start()
        Thread(target=self._log_stderr, daemon=True).start()

    @staticmethod
    def _make_sentinel(host, port, secret):
        """Returns a command to pass to the persistent process, that sends a secret to a socket server listening at
        host/port. Used to wait for commands to finish (see _wait_for_command_to_finish())
        Must be reimplemented in subclasses.

        Args:
            host (str)
            port (int)
            secret (str)

        Returns:
            str
        """
        raise NotImplementedError()

    @property
    def lexer_name(self):
        """Returns a lexer name for syntax highlighting.

        Returns:
            str
        """
        raise NotImplementedError()

    @property
    def prompt(self):
        """Returns a prompt for console applications.

        Returns:
            str
        """
        raise NotImplementedError()

    def issue_command(self, cmd):
        """Issues cmd to the persistent process and returns an iterator of stdout and stderr messages.
        Each message is a dictionary with two keys:
            "type": either "stdout" or "stderr"
            "data": the actual message string.

        Args:
            cmd (str)

        Returns:
            generator
        """
        t = Thread(target=self._issue_command_and_wait_for_finish, args=(cmd,))
        t.start()
        self._idle = False
        while True:
            msg = self._queue.get()
            if msg == self._COMMAND_FINISHED:
                break
            yield msg
        self._idle = True
        t.join()

    def _issue_command_and_wait_for_finish(self, cmd):
        """Issues command and wait for finish."""
        self.command_successful = self._issue_command(cmd) and self._wait_for_command_to_finish()
        self._queue.put(self._COMMAND_FINISHED)

    def _issue_command(self, cmd):
        """Issues command."""
        self._persistent.stdin.write(f"{cmd.strip()}{os.linesep}{os.linesep}".encode("UTF8"))
        try:
            self._persistent.stdin.flush()
            return True
        except BrokenPipeError:
            return False

    def _wait_for_command_to_finish(self):
        """Waits for command to finish. This is implemented by sending the sentinel command to the process
        (see _make_sentinel()) and listening on a socket server until the secret is received.
        """
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
            if msg == secret:
                break
        thread.join()
        return True

    @staticmethod
    def _listen_and_enqueue(host, port, queue):
        """Listens on the server and enqueues all data received."""
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

    def _log_stdout(self):
        """Puts stdout from the process into the queue (it will be consumed by issue_command())."""
        for line in iter(self._persistent.stdout.readline, b''):
            data = line.decode("UTF8", "replace").strip()
            self._queue.put(dict(type="stdout", data=data))

    def _log_stderr(self):
        """Puts stderr from the process into the queue (it will be consumed by issue_command())."""
        for line in iter(self._persistent.stderr.readline, b''):
            data = line.decode("UTF8", "replace").strip()
            self._queue.put(dict(type="stderr", data=data))

    def restart_persistent(self):
        """Restarts the persistent process."""
        self._persistent.kill()
        self._persistent.wait()
        self._persistent = Popen(self._args, **self._kwargs)
        Thread(target=self._log_stdout, daemon=True).start()
        Thread(target=self._log_stderr, daemon=True).start()

    def interrupt_persistent(self):
        """Interrupts the persistent process."""
        if self._persistent is None:
            return
        Thread(target=self._do_interrupt_persistent, args=(), daemon=True).start()

    def _do_interrupt_persistent(self):
        sig = signal.CTRL_C_EVENT if sys.platform == "win32" else signal.SIGINT
        while not self._idle:
            self._persistent.send_signal(sig)
            time.sleep(1)

    def is_persistent_alive(self):
        """Whether or not the persistent is still alive and ready to receive commands.

        Returns:
            bool
        """
        return self._persistent.returncode is None


class JuliaPersistentManager(PersistentManagerBase):
    @staticmethod
    def _make_sentinel(host, port, secret):
        return f'using Sockets; let s = connect("{host}", {port}); write(s, "{secret}"); close(s) end;'

    @property
    def lexer_name(self):
        return "julia"

    @property
    def prompt(self):
        return '<br><span style="color:green; font-weight: bold">julia></span> '


class PythonPersistentManager(PersistentManagerBase):
    @staticmethod
    def _make_sentinel(host, port, secret):
        return textwrap.dedent(
            f"""\
            import socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(("{host}", {port}))
                s.sendall(b"{secret}")
            """
        )

    @property
    def lexer_name(self):
        return "python"

    @property
    def prompt(self):
        return '>>> '


class _PersistentManagerFactory(metaclass=Singleton):
    _persistent_managers = {}
    """Maps tuples (*process args) to associated PersistentManagerBase."""

    def new_persistent_manager(self, constructor, logger, args, group_id, cwd=None):
        """Creates a new persistent for given args and group id if none exists.

        Args:
            constructor (function): the persistent manager constructor
            logger (LoggerInterface)
            args (list): the arguments to launch the persistent process
            group_id (str): item group that will execute using this persistent
            cwd (str, optional): directory where to start the persistent

        Returns:
            Popen
        """
        if group_id is None:
            # Execute in isolation
            return constructor(args, cwd=cwd)
        key = tuple(args + [group_id])
        if key not in self._persistent_managers or not self._persistent_managers[key].is_persistent_alive():
            try:
                self._persistent_managers[key] = constructor(args, cwd=cwd)
            except OSError as err:
                msg = dict(type="persistent_failed_to_start", args=args, error=str(err))
                logger.msg_persistent_execution.emit(msg)
                return None
        pm = self._persistent_managers[key]
        msg = dict(type="persistent_started", key=key, lexer_name=pm.lexer_name, prompt=pm.prompt)
        logger.msg_persistent_execution.emit(msg)
        return pm

    def restart_persistent(self, key):
        """Restart a persistent process.

        Args:
            key (tuple): persistent identifier
        """
        pm = self._persistent_managers.get(key)
        if pm is None:
            return
        pm.restart_persistent()

    def interrupt_persistent(self, key):
        """Interrupts a persistent process.

        Args:
            key (tuple): persistent identifier
        """
        pm = self._persistent_managers.get(key)
        if pm is None:
            return
        pm.interrupt_persistent()

    def issue_persistent_command(self, key, cmd):
        """Issues a command to a persistent process.

        Args:
            key (tuple): persistent identifier
            command (str): command to issue

        Returns:
            generator: stdio and stderr messages (dictionaries with two keys: type, and data)
        """
        pm = self._persistent_managers.get(key)
        if pm is None:
            return
        for msg in pm.issue_command(cmd):
            yield msg


_persistent_manager_factory = _PersistentManagerFactory()


def restart_persistent(key):
    """Restart a persistent process.

    Args:
        key (tuple): persistent identifier
    """
    _persistent_manager_factory.restart_persistent(key)


def interrupt_persistent(key):
    """Interrupts a persistent process.

    Args:
        key (tuple): persistent identifier
    """
    _persistent_manager_factory.interrupt_persistent(key)


def issue_persistent_command(key, cmd):
    """Issues a command to a persistent process.

    Args:
        key (tuple): persistent identifier
        command (str): command to issue

    Returns:
        generator: stdio and stderr messages (dictionaries with two keys: type, and data)
    """
    yield from _persistent_manager_factory.issue_persistent_command(key, cmd)


class PersistentExecutionManagerBase(ExecutionManagerBase):
    """Base class for managing execution of commands on a persistent process."""

    def __init__(self, logger, args, commands, group_id=None, workdir=None):
        """Class constructor.

        Args:
            logger (LoggerInterface): a logger instance
            args (list): List of args to start the persistent process
            commands (list): List of commands to execute in the persistent process
            group_id (str, optional): item group that will execute using this kernel
            workdir (str, optional): item group that will execute using this kernel
        """
        super().__init__(logger)
        self._commands = commands
        self._persistent_manager = _persistent_manager_factory.new_persistent_manager(
            self.persistent_manager_factory(), logger, args, group_id, cwd=workdir
        )

    @staticmethod
    def persistent_manager_factory():
        """Returns a function to create a persistent manager for this execution
        (a subclass of PersistentManagerBase)

        Returns:
            function
        """
        raise NotImplementedError()

    def run_until_complete(self):
        """See base class."""
        for cmd in self._commands:
            self._logger.msg_persistent_execution.emit(dict(type="stdin", data=cmd.strip()))
            for msg in self._persistent_manager.issue_command(cmd):
                self._logger.msg_persistent_execution.emit(msg)
            if not self._persistent_manager.command_successful:
                self._logger.msg_proc_error.emit(f"Error executing {cmd}")
                return -1
        return 0

    def stop_execution(self):
        """See base class."""
        self._persistent_manager.interrupt_persistent()


class JuliaPersistentExecutionManager(PersistentExecutionManagerBase):
    """Manages execution of commands on a Julia persistent process."""

    @staticmethod
    def persistent_manager_factory():
        """See base class."""
        return JuliaPersistentManager


class PythonPersistentExecutionManager(PersistentExecutionManagerBase):
    """Manages execution of commands on a Python persistent process."""

    @staticmethod
    def persistent_manager_factory():
        """See base class."""
        return PythonPersistentManager
