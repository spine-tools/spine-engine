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
import time
from subprocess import Popen, PIPE
from threading import Thread
from multiprocessing import Lock
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
        self._msg_queue = Queue()
        self.command_successful = False
        self._kwargs = dict(stdin=PIPE, stdout=PIPE, stderr=PIPE, cwd=cwd)
        if sys.platform == "win32":
            self._kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP
        self._lock = Lock()
        self._start_persistent()

    @property
    def language(self):
        """Returns the underlying language for UI customization in toolbox.

        Returns:
            str
        """
        raise NotImplementedError()

    @staticmethod
    def _init_args():
        """Returns init args for Popen. Subclasses must reimplement to include appropriate switches to ensure the
        process is interactive, or to load modules for the sentinel command.

        Returns:
            list
        """
        raise NotImplementedError()

    @staticmethod
    def _message_command(host, port, msg):
        """Returns a command that sends a message to a socket server listening at host/port.
        Used to communicate with the persistent process (see _communicate()).
        Must be reimplemented in subclasses.

        Args:
            host (str)
            port (int)
            msg (str): Code in the underlying language, that returns a str to send through the socket

        Returns:
            str
        """
        raise NotImplementedError()

    @staticmethod
    def _completions_command(text):
        """Returns a command that returns a space separated list of completion options.

        Args:
            text (str): text to complete

        Returns:
            str: command
        """
        raise NotImplementedError()

    def _start_persistent(self):
        """Starts the persistent process."""
        self.command_successful = False
        self._persistent = Popen(self._args + self._init_args(), **self._kwargs)
        Thread(target=self._log_stdout, daemon=True).start()
        Thread(target=self._log_stderr, daemon=True).start()

    def _log_stdout(self):
        """Puts stdout from the process into the queue (it will be consumed by issue_command())."""
        for line in iter(self._persistent.stdout.readline, b''):
            data = line.decode("UTF8", "replace").strip()
            self._msg_queue.put(dict(type="stdout", data=data))

    def _log_stderr(self):
        """Puts stderr from the process into the queue (it will be consumed by issue_command())."""
        for line in iter(self._persistent.stderr.readline, b''):
            data = line.decode("UTF8", "replace").strip()
            self._msg_queue.put(dict(type="stderr", data=data))

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
        while True:
            msg = self._msg_queue.get()
            if msg == self._COMMAND_FINISHED:
                break
            yield msg
        t.join()

    def _issue_command_and_wait_for_finish(self, cmd):
        """Issues command and wait for finish."""
        with self._lock:
            self.command_successful = self._issue_command(cmd) and self._communicate('"idle"') == "idle"
        self._msg_queue.put(self._COMMAND_FINISHED)

    def _issue_command(self, cmd):
        """Writes command to the process's stdin and flushes."""
        self._persistent.stdin.write(f"{cmd.strip()}{os.linesep}{os.linesep}".encode("UTF8"))
        try:
            self._persistent.stdin.flush()
            return True
        except BrokenPipeError:
            return False

    def get_completions(self, text):
        """Returns a list of autocompletion options for given text.

        Args:
            text (str): Text to complete

        Returns:
            list(str): List of options
        """
        cmd = self._completions_command(text)
        return self._communicate(cmd).strip().split(" ")

    def _communicate(self, msg):
        """Communicates with persistent process. Sends a message through a socket and returns the response.

        Args:
            msg (str)

        Returns:
            str
        """
        host = "127.0.0.1"
        with socketserver.TCPServer((host, 0), None) as s:
            port = s.server_address[1]
        queue = Queue()
        thread = Thread(target=self._listen_and_enqueue, args=(host, port, queue))
        thread.start()
        queue.get()  # This blocks until the server is listening
        msg_cmd = self._message_command(host, port, msg)
        if not self._issue_command(msg_cmd):
            thread.join()
            return None
        response = queue.get()
        thread.join()
        return response

    @staticmethod
    def _listen_and_enqueue(host, port, queue):
        """Listens on the server and enqueues all data received."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, port))
            s.listen()
            queue.put(None)
            conn, _ = s.accept()
            with conn:
                while True:
                    data = conn.recv(1024)
                    queue.put(data.decode("UTF8", "replace"))
                    if not data:
                        break

    def restart_persistent(self):
        """Restarts the persistent process."""
        self._persistent.kill()
        self._persistent.wait()
        self._persistent.stdout = os.devnull
        self._persistent.stderr = os.devnull
        self._start_persistent()

    def interrupt_persistent(self):
        """Interrupts the persistent process."""
        if self._persistent is None:
            return
        Thread(target=self._do_interrupt_persistent).start()

    def _do_interrupt_persistent(self):
        sig = signal.CTRL_C_EVENT if sys.platform == "win32" else signal.SIGINT
        for _ in range(3):
            self._persistent.send_signal(sig)
            time.sleep(1)

    def is_persistent_alive(self):
        """Whether or not the persistent is still alive and ready to receive commands.

        Returns:
            bool
        """
        return self._persistent.returncode is None


class JuliaPersistentManager(PersistentManagerBase):
    @property
    def language(self):
        """See base class."""
        return "julia"

    @staticmethod
    def _init_args():
        """See base class."""
        return ["-i", "-e", "using Sockets, REPL.REPLCompletions"]

    @staticmethod
    def _message_command(host, port, msg):
        """See base class."""
        return f'let s = connect("{host}", {port}); write(s, {msg}); close(s) end;'

    @staticmethod
    def _completions_command(text):
        """See base class."""
        return f'join(completion_text.(completions("{text}", {len(text)})[1]), " ")'


class PythonPersistentManager(PersistentManagerBase):
    @property
    def language(self):
        """See base class."""
        return "python"

    @staticmethod
    def _init_args():
        """See base class."""
        return ["-q", "-i", "-c", "import socket, sys, readline, itertools; sys.ps1 = sys.ps2 = ''"]

    @staticmethod
    def _message_command(host, port, msg):
        """See base class."""
        body = f's.connect(("{host}", {port})) or s.sendall({msg}.encode("UTF8")) or s.close()'
        s = "socket.socket(socket.AF_INET, socket.SOCK_STREAM)"
        return f'(lambda s={s}: {body})()'  # Avoid creating any variables

    @staticmethod
    def _completions_command(text):
        """See base class."""
        return f'" ".join(itertools.takewhile(bool, (readline.get_completer()("{text}", k) for k in range(100))))'


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
                msg = dict(type="persistent_failed_to_start", args=" ".join(args), error=str(err))
                logger.msg_persistent_execution.emit(msg)
                return None
        pm = self._persistent_managers[key]
        msg = dict(type="persistent_started", key=key, language=pm.language)
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

    def get_persistent_completions(self, key, text):
        pm = self._persistent_managers.get(key)
        if pm is None:
            return
        return pm.get_completions(text)


_persistent_manager_factory = _PersistentManagerFactory()


def restart_persistent(key):
    """See _PersistentManagerFactory."""
    _persistent_manager_factory.restart_persistent(key)


def interrupt_persistent(key):
    """See _PersistentManagerFactory."""
    _persistent_manager_factory.interrupt_persistent(key)


def issue_persistent_command(key, cmd):
    """See _PersistentManagerFactory."""
    yield from _persistent_manager_factory.issue_persistent_command(key, cmd)


def get_persistent_completions(key, text):
    """See _PersistentManagerFactory."""
    return _persistent_manager_factory.get_persistent_completions(key, text)


class PersistentExecutionManagerBase(ExecutionManagerBase):
    """Base class for managing execution of commands on a persistent process."""

    def __init__(self, logger, args, commands, alias, group_id=None, workdir=None):
        """Class constructor.

        Args:
            logger (LoggerInterface): a logger instance
            args (list): List of args to start the persistent process
            commands (list): List of commands to execute in the persistent process
            group_id (str, optional): item group that will execute using this kernel
            workdir (str, optional): item group that will execute using this kernel
        """
        super().__init__(logger)
        self._args = args
        self._commands = commands
        self._alias = alias
        self._persistent_manager = _persistent_manager_factory.new_persistent_manager(
            self.persistent_manager_factory(), logger, args, group_id, cwd=workdir
        )

    @property
    def alias(self):
        return self._alias

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
        msg = dict(type="execution_started", args=" ".join(self._args))
        self._logger.msg_persistent_execution.emit(msg)
        self._logger.msg_persistent_execution.emit(dict(type="stdin", data=self._alias.strip()))
        for cmd in self._commands:
            for msg in self._persistent_manager.issue_command(cmd):
                self._logger.msg_persistent_execution.emit(msg)
            if not self._persistent_manager.command_successful:
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
