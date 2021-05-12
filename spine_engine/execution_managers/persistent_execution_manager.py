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

    _SENTINEL = '\uf056'
    """Random character in unicode private space.
    Used to 'mark' stdout lines for internal communication with the process.
    (E.g., to check that the process is 'idle', or to retrieve autocompletion options.)
    It is assumed that user commands *never* produce stdout lines that start with this character.
    (I guess the chances of that happening are low, but never zero?)
    """

    def __init__(self, args, cwd=None):
        """
        Args:
            args (list): the arguments to launch the persistent process
            cwd (str, optional): the directory where to start the process
        """
        self._args = args
        self._msg_queue = Queue()
        self._cmd_queue = Queue()
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
        process is interactive, or to load modules for internal communication.

        Returns:
            list
        """
        raise NotImplementedError()

    @staticmethod
    def _completions_command(text):
        """Returns a command for getting a space separated list of completion options.

        Args:
            text (str): text to complete

        Returns:
            str: command
        """

    def _encode_command(self, cmd):
        """Returns a command that prints the output of given command prepended by the sentinel character.
        Used to communicate internally with the process.

        Args:
            cmd (str)

        Returns:
            str
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
            if data.startswith(self._SENTINEL):
                self._cmd_queue.put(data[1:])
                continue
            self._msg_queue.put(dict(type="stdout", data=data))

    def _log_stderr(self):
        """Puts stderr from the process into the queue (it will be consumed by issue_command())."""
        for line in iter(self._persistent.stderr.readline, b''):
            data = line.decode("UTF8", "replace").strip()
            self._msg_queue.put(dict(type="stderr", data=data))

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
            if msg == self._COMMAND_FINISHED:
                break
            yield msg
        t.join()

    def _issue_command_and_wait_for_idle(self, cmd, add_history):
        """Issues command and wait for idle."""
        with self._lock:
            self.command_successful = self._issue_command(cmd, add_history) and self._communicate('"done"') == "done"
        self._msg_queue.put(self._COMMAND_FINISHED)

    def _issue_command(self, cmd, add_history=False):
        """Writes command to the process's stdin and flushes."""
        self._persistent.stdin.write(f"{cmd.strip()}{os.linesep}{os.linesep}".encode("UTF8"))
        try:
            self._persistent.stdin.flush()
            if add_history:
                self._communicate(self._add_history_command(cmd))
            return True
        except BrokenPipeError:
            return False

    def _communicate(self, cmd):
        """Communicates with the persistent process internally. Sends given command and returns output.

        Args:
            cmd (str)

        Returns:
            str
        """
        cmd = self._encode_command(cmd)
        if not self._issue_command(cmd):
            return None
        return self._cmd_queue.get()

    def get_completions(self, text):
        """Returns a list of autocompletion options for given text.

        Args:
            text (str): Text to complete

        Returns:
            list(str): List of options
        """
        cmd = self._completions_command(text)
        result = self._communicate(cmd)
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
        cmd = self._history_item_command(index)
        return self._communicate(cmd).strip()

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
        return ["-i", "-e", "using REPL.REPLCompletions"]

    def _encode_command(self, cmd):
        """See base class."""
        return f'println("{self._SENTINEL}", {cmd})'

    @staticmethod
    def _completions_command(text):
        """See base class."""
        return f'join(completion_text.(completions("{text}", {len(text)})[1]), " ")'

    @staticmethod
    def _history_item_command(index):
        """See base class."""
        return f"Base.active_repl.mistate.current_mode.hist.history[end - {index}]"

    @staticmethod
    def _add_history_command(line):
        return ""


class PythonPersistentManager(PersistentManagerBase):
    @property
    def language(self):
        """See base class."""
        return "python"

    @staticmethod
    def _init_args():
        """See base class."""
        return [
            "-q",
            "-i",
            "-c",
            "import itertools, sys; sys.ps1 = sys.ps2 = '';\ntry:\n\timport readline\nexcept:\n\tpass; ",
        ]  # Remove prompts

    def _encode_command(self, cmd):
        """See base class."""
        return f'try:\n\tprint("{self._SENTINEL}" + {cmd})\nexcept:\n\tprint("{self._SENTINEL}")'

    @staticmethod
    def _completions_command(text):
        """See base class."""
        return f'" ".join(itertools.takewhile(bool, (readline.get_completer()("{text}", k) for k in range(100))))'

    @staticmethod
    def _add_history_command(line):
        line = line.replace('"', '\\"')
        return f'readline.add_history("{line}")'

    @staticmethod
    def _history_item_command(index):
        """See base class."""
        return f"readline.get_history_item(readline.get_current_history_length() + 1 - {index})"


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
