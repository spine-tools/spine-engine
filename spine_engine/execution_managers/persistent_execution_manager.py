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

import socket
import socketserver
import sys
import os
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
    def __init__(self, args, cwd=None):
        """
        Args:
            args (list): the arguments to launch the persistent process
            cwd (str, optional): the directory where to start the process
        """
        self._args = args
        self._command_buffer = []
        self._server_address = None
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

    def _init_args(self):
        """Returns init args for Popen. Subclasses must reimplement to include appropriate switches to ensure the
        process is interactive, or to load modules for internal communication.

        Returns:
            list
        """
        raise NotImplementedError()

    @staticmethod
    def _sentinel_command(host, port):
        """Returns a command in the underlying language, that sends a sentinel to a socket listening on given
        host/port.
        Used to synchronize with the persistent process.

        Args:
            host (str)
            port (int)

        Returns:
            str
        """
        raise NotImplementedError()

    def _start_persistent(self):
        """Starts the persistent process."""
        host = "127.0.0.1"
        with socketserver.TCPServer((host, 0), None) as s:
            self._server_address = s.server_address
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

    def _is_complete(self, cmd):
        result = self._communicate("is_complete", cmd)
        return result.strip() == "true"

    def issue_command(self, cmd, add_history=False):
        """Issues cmd to the persistent process and returns an iterator of stdout and stderr messages.
        Each message is a dictionary with two keys:
            "type": either "stdout" or "stderr"
            "data": the actual message string.

        Args:
            cmd (str)

        Returns:
            generator
        """
        t = Thread(target=self._issue_command_and_wait_for_idle, args=(cmd, add_history))
        t.start()
        while True:
            msg = self._msg_queue.get()
            yield msg
            if msg["type"] == "command_finished":
                break
        t.join()

    def _make_complete_command(self):
        complete_cmd = os.linesep.join(self._command_buffer)
        if len(self._command_buffer) == 1:
            complete_cmd += os.linesep
        return complete_cmd

    def _issue_command_and_wait_for_idle(self, cmd, add_history):
        """Issues command and wait for idle.

        Args:
            cmd (str): Command to pass to the persistent process
            add_history (bool): Whether or not to add the command to history
        """
        with self._lock:
            self._msg_queue.put({"type": "stdin", "data": cmd})
            self._command_buffer.append(cmd)
            complete_cmd = self._make_complete_command()
            is_complete = self._is_complete(complete_cmd)
            self.command_successful = self._issue_command(cmd, is_complete, add_history)
            if self.command_successful and is_complete:
                self.command_successful &= self._wait()
                self._command_buffer.clear()
        self._msg_queue.put({"type": "command_finished", "is_complete": is_complete})

    def _issue_command(self, cmd, is_complete=True, add_history=False):
        """Writes command to the process's stdin and flushes.

        Args:
            cmd (str): Command to pass to the persistent process
            is_complete (bool): Whether or not the command is complete
            add_history (bool): Whether or not to add the command to history
        """
        cmd += os.linesep
        if is_complete:
            cmd += os.linesep
        self._persistent.stdin.write(cmd.encode("UTF8"))
        try:
            self._persistent.stdin.flush()
            if is_complete and add_history:
                complete_cmd = self._make_complete_command()
                self._communicate("add_history", complete_cmd, receive=False)
            return True
        except BrokenPipeError:
            return False

    def _wait(self):
        """Waits for the persistent process to become idle.
        This is implemented by writing the sentinel to stdin and waiting for the _SENTINEL to come out of stdout.

        Returns:
            bool
        """
        host = "127.0.0.1"
        with socketserver.TCPServer((host, 0), None) as s:
            port = s.server_address[1]
        queue = Queue()
        thread = Thread(target=self._listen_and_enqueue, args=(host, port, queue))
        thread.start()
        queue.get()  # This blocks until the server is listening
        sentinel = self._sentinel_command(host, port)
        if not self._issue_command(sentinel):
            thread.join()
            return False
        queue.get()
        thread.join()
        return True

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

    def _communicate(self, request, arg, receive=True):
        """
        Sends a request to the persistent process with the given argument.

        Args:
            request (str): One of the supported requests
            arg: Request argument
            receive (bool, optional): If True (the default) also receives the response and returns it.

        Returns:
            str or NoneType: response, or None if the ``receive`` argument is False
        """
        msg = f"{request};;{arg}"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            while True:
                try:
                    s.connect(self._server_address)
                    break
                except ConnectionRefusedError:
                    time.sleep(0.02)
            s.sendall(bytes(msg, "UTF8"))
            if receive:
                response = s.recv(1000000)
                return str(response, "UTF8")

    def get_completions(self, text):
        """Returns a list of autocompletion options for given text.

        Args:
            text (str): Text to complete

        Returns:
            list(str): List of options
        """
        result = self._communicate("completions", text)
        if result is None:
            return []
        return result.strip().split(" ")

    def get_history_item(self, index):
        """Returns the history item given by index.

        Args:
            index (int): Index to retrieve, one-based, 1 means most recent

        Returns:
            str
        """
        if index < 1:
            return ""
        return self._communicate("history_item", index).strip()

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

    def kill_process(self):
        self._persistent.kill()
        self._persistent.communicate(timeout=5)


class JuliaPersistentManager(PersistentManagerBase):
    @property
    def language(self):
        """See base class."""
        return "julia"

    def _init_args(self):
        """See base class."""
        path = os.path.join(os.path.dirname(__file__), "spine_repl.jl").replace(os.sep, "/")
        host, port = self._server_address
        return ["-i", "-e", f'include("{path}"); SpineREPL.start_server("{host}", {port})']

    @staticmethod
    def _sentinel_command(host, port):
        return f'SpineREPL.send_sentinel("{host}", {port})'


class PythonPersistentManager(PersistentManagerBase):
    @property
    def language(self):
        """See base class."""
        return "python"

    def _init_args(self):
        """See base class."""
        path = os.path.dirname(__file__).replace(os.sep, "/")
        return [
            "-q",
            "-i",
            "-c",
            f"import sys; sys.ps1 = sys.ps2 = ''; sys.path.append('{path}'); "
            f"import spine_repl; spine_repl.start_server({self._server_address})",
        ]

    @staticmethod
    def _sentinel_command(host, port):
        return f'spine_repl.send_sentinel("{host}", {port})'


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
            PersistentManagerBase: persistent manager
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
            cmd (str): command to issue

        Returns:
            generator: stdio and stderr messages (dictionaries with two keys: type, and data)
        """
        pm = self._persistent_managers.get(key)
        if pm is None:
            return
        for msg in pm.issue_command(cmd, add_history=True):
            yield msg

    def get_persistent_completions(self, key, text):
        """Returns a list of completion options.

        Args:
            key (tuple): persistent identifier
            text (str): text to complete

        Returns:
            list of str: options that match given text
        """
        pm = self._persistent_managers.get(key)
        if pm is None:
            return
        return pm.get_completions(text)

    def get_persistent_history_item(self, key, index):
        """Issues a command to a persistent process.

        Args:
            key (tuple): persistent identifier
            index (int): index of the history item, most recen first

        Returns:
            str: history item or empty string if none
        """
        pm = self._persistent_managers.get(key)
        if pm is None:
            return
        return pm.get_history_item(index)

    def kill_manager_processes(self):
        """Kills persistent managers' Popen instances."""
        for manager in self._persistent_managers.values():
            manager.kill_process()


_persistent_manager_factory = _PersistentManagerFactory()


def restart_persistent(key):
    """See _PersistentManagerFactory."""
    _persistent_manager_factory.restart_persistent(key)


def interrupt_persistent(key):
    """See _PersistentManagerFactory."""
    _persistent_manager_factory.interrupt_persistent(key)


def kill_persistent_processes():
    """Kills all persistent processes.

    On Windows systems the work directories are reserved by the ``Popen`` objects owned by the persistent managers.
    They need to be killed to before the directories can be modified, e.g. deleted or renamed.
    """
    _persistent_manager_factory.kill_manager_processes()


def issue_persistent_command(key, cmd):
    """See _PersistentManagerFactory."""
    yield from _persistent_manager_factory.issue_persistent_command(key, cmd)


def get_persistent_completions(key, text):
    """See _PersistentManagerFactory."""
    return _persistent_manager_factory.get_persistent_completions(key, text)


def get_persistent_history_item(key, index):
    """See _PersistentManagerFactory."""
    return _persistent_manager_factory.get_persistent_history_item(key, index)


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
                if msg["type"] != "stdin":
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
