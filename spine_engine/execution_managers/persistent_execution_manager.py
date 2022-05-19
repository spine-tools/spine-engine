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
import contextlib
import socket
import socketserver
import sys
import os
import signal
import threading
import time
from dataclasses import dataclass
from itertools import chain
from subprocess import Popen, PIPE
from multiprocessing import Process, Lock
from queue import Queue, Empty
from ..utils.helpers import Singleton
from ..utils.execution_resources import persistent_process_semaphore
from .execution_manager_base import ExecutionManagerBase

if sys.platform == "win32":
    import ctypes
    from subprocess import CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW


class PersistentManagerBase:
    def __init__(self, args):
        """
        Args:
            args (list): the arguments to launch the persistent process
        """
        self._args = args
        self._server_address = None
        self._msg_queue = Queue()
        self.command_successful = False
        self._is_running_lock = Lock()
        self._is_running = True
        self._kwargs = dict(stdin=PIPE, stdout=PIPE, stderr=PIPE)
        if sys.platform == "win32":
            self._kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
            # Setup Popen to not show console in frozen app. Another option is to use
            # startupinfo argument and STARTUPINFO() class.
        self._lock = Lock()
        self._persistent = None
        self._start_persistent()
        self._wait()

    @property
    def language(self):
        """Returns the underlying language for UI customization in toolbox.

        Returns:
            str
        """
        raise NotImplementedError()

    def set_running_until_completion(self, running):
        """Sets the 'running until completion' state.

        Args:
            running (bool): True if the manager is running until completion, False otherwise
        """
        with self._is_running_lock:
            self._is_running = running

    def is_running_until_completion(self):
        with self._is_running_lock:
            return self._is_running

    def _init_args(self):
        """Returns init args for Popen.

        Subclasses must reimplement to include appropriate switches to ensure the
        process is interactive, or to load modules for internal communication.

        Returns:
            list
        """
        raise NotImplementedError()

    @staticmethod
    def _catch_exception_command(cmd):
        """Returns an equivalent command that also recorders if an exception is raised.

        Args:
            cmd (str)

        Returns:
            str
        """
        raise NotImplementedError()

    @staticmethod
    def _ping_command(host, port):
        """Returns a command in the underlying language that connects to a socket listening on given host/port,
        and sends "ok" if no exception has been recorded.

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
        threading.Thread(target=self._log_stdout, daemon=True).start()
        threading.Thread(target=self._log_stderr, daemon=True).start()

    def _log_stdout(self):
        """Puts stdout from the process into the queue (it will be consumed by issue_command())."""
        for line in iter(self._persistent.stdout.readline, b''):
            data = line.decode("UTF8", "replace").rstrip()
            self._msg_queue.put(dict(type="stdout", data=data))

    def _log_stderr(self):
        """Puts stderr from the process into the queue (it will be consumed by issue_command())."""
        for line in iter(self._persistent.stderr.readline, b''):
            data = line.decode("UTF8", "replace").rstrip()
            self._msg_queue.put(dict(type="stderr", data=data))

    def make_complete_command(self, cmd):
        lines = cmd.splitlines()
        cmd = os.linesep.join(lines)
        if len(lines) == 1:
            cmd += os.linesep
        result = self._communicate("is_complete", cmd)
        if result.strip() != "true":
            return None
        return cmd

    def drain_queue(self):
        while True:
            try:
                yield self._msg_queue.get(timeout=0.02)
            except Empty:
                break

    def issue_command(self, cmd, add_history=False, catch_exception=True):
        """Issues cmd to the persistent process and yields stdout and stderr messages.

        Each message is a dictionary with two keys:

            - "type": message type, e.g. "stdout" or "stderr"
            - "data": the actual message string.

        Args:
            cmd (str)
            add_history (bool)

        Yields:
            dict: message
        """
        cmd = self.make_complete_command(cmd)
        if cmd is None:
            return
        t = threading.Thread(target=self._issue_command_and_wait_for_idle, args=(cmd, add_history, catch_exception))
        t.start()
        while True:
            msg = self._msg_queue.get()
            if msg["type"] == "command_finished":
                break
            yield msg
        t.join()

    def _issue_command_and_wait_for_idle(self, cmd, add_history, catch_exception):
        """Issues command and wait for idle.

        Args:
            cmd (str): Command to pass to the persistent process
            add_history (bool): Whether or not to add the command to history
        """
        with self._lock:
            self._msg_queue.put({"type": "stdin", "data": cmd})
            self.command_successful = self._issue_command(cmd, catch_exception=catch_exception)
            if self.command_successful:
                if add_history:
                    self._communicate("add_history", cmd, receive=False)
                self.command_successful &= self._wait()
        self._msg_queue.put({"type": "command_finished"})

    def _issue_command(self, cmd, catch_exception=True):
        """Writes command to the process's stdin and flushes.

        Args:
            cmd (str): Command to pass to the persistent process

        Returns:
            bool: True if command was issued successfully, False otherwise
        """
        if catch_exception:
            cmd = self._catch_exception_command(cmd) if cmd else ""
        cmd += os.linesep
        self._persistent.stdin.write(cmd.encode("UTF8"))
        try:
            self._persistent.stdin.flush()
            return True
        except BrokenPipeError:
            return False

    def _wait(self):
        """Waits for the persistent process to become idle.

        This is implemented by running a socket server that waits for a ping on the current process,
        and then issuing a command to the persistent process that pings that server.
        The ping command will run on the persistent process when it becomes idle and we will catch that situation
        in the server.

        Returns:
            bool
        """
        host = "127.0.0.1"
        with socketserver.TCPServer((host, 0), None) as s:
            port = s.server_address[1]
        queue = Queue()
        thread = threading.Thread(target=self._wait_ping, args=(host, port, queue))
        thread.start()
        queue.get()  # This blocks until the server is listening
        ping = self._ping_command(host, port)
        if not self._issue_command(ping, catch_exception=False):
            thread.join()
            return False
        result = queue.get()  # This blocks until the server is done
        thread.join()
        if not self.is_persistent_alive():
            self._msg_queue.put({"type": "stdout", "data": "Kernel died (×_×)"})
            return self._persistent.returncode == 0
        return result == "ok"

    def _wait_ping(self, host, port, queue, timeout=1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(timeout)
            server.bind((host, port))
            server.listen()
            queue.put(None)
            while True:
                try:
                    conn, _ = server.accept()
                    break
                except socket.timeout:
                    if not self.is_persistent_alive():
                        queue.put("error")
                        return
            conn.settimeout(timeout)
            with conn:
                while True:
                    try:
                        data = conn.recv(1024)
                    except socket.timeout:
                        continue
                    queue.put(data.decode("UTF8", "replace"))
                    if not data:
                        break

    def _communicate(self, request, *args, receive=True):
        """
        Sends a request to the persistent process with the given argument.

        Args:
            request (str): One of the supported requests
            args: Request argument
            receive (bool, optional): If True (the default) also receives the response and returns it.

        Returns:
            str or NoneType: response, or None if the ``receive`` argument is False
        """
        req_args_sep = '\u001f'  # Unit separator
        args_sep = '\u0091'  # Private Use 1
        args = args_sep.join(args)
        msg = f"{request}{req_args_sep}{args}"
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

    def get_history_item(self, text, prefix, backwards):
        """Returns the history item given by index.

        Args:
            text (str): text in the line edit when the search starts
            prefix (str): return the next item that starts with this
            backwards (bool): whether to move the history backwards or not

        Returns:
            str
        """
        sense = "backwards" if backwards else "forward"
        return self._communicate("history_item", text, prefix, sense).strip()

    def restart_persistent(self):
        """Restarts the persistent process."""
        self._persistent.kill()
        self._persistent.wait()
        self._persistent.stdout = os.devnull
        self._persistent.stderr = os.devnull
        self.set_running_until_completion(False)
        self._start_persistent()
        self._wait()
        yield from self.drain_queue()

    def interrupt_persistent(self):
        """Interrupts the persistent process."""
        threading.Thread(target=self._do_interrupt_persistent).start()
        self._wait()

    def _do_interrupt_persistent(self):
        persistent = self._persistent  # Make local copy; other threads may set self._persistent to None while sleeping.
        if persistent is None:
            return
        if sys.platform == "win32":
            p = Process(target=_send_ctrl_c, args=(persistent.pid,))
            p.start()
            p.join()
        else:
            persistent.send_signal(signal.SIGINT)
        self.set_running_until_completion(False)

    def is_persistent_alive(self):
        """Whether or not the persistent is still alive and ready to receive commands.

        Returns:
            bool
        """
        return self._persistent is not None and self._persistent.poll() is None

    def kill_process(self):
        if self._persistent is None:
            return
        self._persistent.kill()
        self._persistent.wait()
        self._persistent.stdin.close()
        self._persistent.stdout.close()
        self._persistent.stderr.close()
        self._persistent = None
        self.set_running_until_completion(False)
        persistent_process_semaphore.release()


def _send_ctrl_c(pid):
    kernel = ctypes.windll.kernel32
    kernel.FreeConsole()
    kernel.AttachConsole(pid)
    kernel.SetConsoleCtrlHandler(None, 1)
    kernel.GenerateConsoleCtrlEvent(0, 0)
    sys.exit(0)


class JuliaPersistentManager(PersistentManagerBase):
    @property
    def language(self):
        """See base class."""
        return "julia"

    def _init_args(self):
        """See base class."""
        path = os.path.join(os.path.dirname(__file__), "spine_repl.jl").replace(os.sep, "/")
        host, port = self._server_address
        return [
            "-i",
            "-e",
            f'include("{path}"); SpineREPL.start_server("{host}", {port})',
            "--color=yes",
            "--banner=yes",
        ]

    @staticmethod
    def _catch_exception_command(cmd):
        return f"try SpineREPL.set_exception(false); @eval {cmd} catch; SpineREPL.set_exception(true); rethrow() end"

    @staticmethod
    def _ping_command(host, port):
        return f'SpineREPL.ping("{host}", {port})'


class PythonPersistentManager(PersistentManagerBase):
    @property
    def language(self):
        """See base class."""
        return "python"

    def _init_args(self):
        """See base class."""
        path = os.path.dirname(__file__).replace(os.sep, "/")
        return [
            "-i",
            "-u",
            "-c",
            f"import sys; sys.ps1 = sys.ps2 = ''; sys.path.append('{path}'); "
            f"import spine_repl; spine_repl.start_server({self._server_address})",
        ]

    @staticmethod
    def _catch_exception_command(cmd):
        cmd_lines = cmd.splitlines()
        indent = "\t" if any(l.startswith("\t") for l in cmd_lines) else "  "
        lines = ["try:"]
        lines += [indent + "spine_repl.set_exception(False)"]
        for l in cmd_lines:
            lines += [indent + l]
        lines += ["except:"]
        lines += [indent + "spine_repl.set_exception(True)"]
        lines += [indent + "raise"]
        return os.linesep.join(lines) + os.linesep

    @staticmethod
    def _ping_command(host, port):
        return f'spine_repl.ping("{host}", {port})'


class _PersistentManagerFactory(metaclass=Singleton):
    @dataclass
    class OpenSign:
        open: bool = True

        def __bool__(self):
            return self.open

    persistent_managers = {}
    """Maps tuples (process args) to associated PersistentManagerBase."""
    _isolated_managers = []
    factory_lock = threading.Lock()
    _factory_open = OpenSign()

    def new_persistent_manager(self, constructor, logger, args, group_id):
        """Creates a new persistent for given args and group id if none exists.

        Args:
            constructor (Callable): the persistent manager constructor
            logger (LoggerInterface)
            args (list): the arguments to launch the persistent process
            group_id (str): item group that will execute using this persistent

        Returns:
            PersistentManagerBase: persistent manager or None if factory has been closed
        """
        with self.factory_lock:
            if group_id is None:
                # Execute in isolation
                with acquire_persistent_process(self.persistent_managers, self._isolated_managers, self._factory_open):
                    if not self._factory_open:
                        return None
                    mp = constructor(args)
                    self._isolated_managers.append(mp)
                    return mp
            key = tuple(args + [group_id])
            if key not in self.persistent_managers or not self.persistent_managers[key].is_persistent_alive():
                with acquire_persistent_process(self.persistent_managers, self._isolated_managers, self._factory_open):
                    if not self._factory_open:
                        return None
                    try:
                        self.persistent_managers[key] = pm = constructor(args)
                    except OSError as err:
                        msg = dict(type="persistent_failed_to_start", args=" ".join(args), error=str(err))
                        logger.msg_persistent_execution.emit(msg)
                        return None
            else:
                pm = self.persistent_managers[key]
            msg = dict(type="persistent_started", key=key, language=pm.language)
            logger.msg_persistent_execution.emit(msg)
            for msg in pm.drain_queue():
                logger.msg_persistent_execution.emit(msg)
            return pm

    def restart_persistent(self, key):
        """Restart a persistent process.

        Args:
            key (tuple): persistent identifier
        Returns:
            generator: stdout and stderr messages (dictionaries with two keys: type, and data)
        """
        pm = self.persistent_managers.get(key)
        if pm is None:
            return ()
        yield from pm.restart_persistent()

    def interrupt_persistent(self, key):
        """Interrupts a persistent process.

        Args:
            key (tuple): persistent identifier
        """
        pm = self.persistent_managers.get(key)
        if pm is None:
            return
        pm.interrupt_persistent()

    def issue_persistent_command(self, key, cmd):
        """Issues a command to a persistent process.

        Args:
            key (tuple): persistent identifier
            cmd (str): command to issue

        Returns:
            generator: stdin, stdout, and stderr messages (dictionaries with two keys: type, and data)
        """
        pm = self.persistent_managers.get(key)
        if pm is None:
            return ()
        for msg in pm.issue_command(cmd, add_history=True, catch_exception=False):
            yield msg

    def is_persistent_command_complete(self, key, cmd):
        """Checkes whether a command is complete.

        Args:
            key (tuple): persistent identifier
            cmd (str): command to issue

        Returns:
            bool
        """
        pm = self.persistent_managers.get(key)
        if pm is None:
            return False
        return pm.make_complete_command(cmd) is not None

    def get_persistent_completions(self, key, text):
        """Returns a list of completion options.

        Args:
            key (tuple): persistent identifier
            text (str): text to complete

        Returns:
            list of str: options that match given text
        """
        pm = self.persistent_managers.get(key)
        if pm is None:
            return
        return pm.get_completions(text)

    def get_persistent_history_item(self, key, text, prefix, backwards):
        """Returns a history item.

        Args:
            key (tuple): persistent identifier

        Returns:
            str: history item or empty string if none
        """
        pm = self.persistent_managers.get(key)
        if pm is None:
            return
        return pm.get_history_item(text, prefix, backwards)

    def kill_manager_processes(self):
        """Kills persistent managers' Popen instances."""
        for manager in chain(self.persistent_managers.values(), self._isolated_managers):
            manager.kill_process()

    def open_factory(self):
        """Opens factory.

        An open factory instantiates new persistent managers.
        """
        self._factory_open.open = True

    def close_factory(self):
        """Closes the factory.

        A closed factory does not instantiate new persistent managers
        """
        self._factory_open.open = False


_persistent_manager_factory = _PersistentManagerFactory()


def restart_persistent(key):
    """See _PersistentManagerFactory."""
    yield from _persistent_manager_factory.restart_persistent(key)


def interrupt_persistent(key):
    """See _PersistentManagerFactory."""
    _persistent_manager_factory.interrupt_persistent(key)


def kill_persistent_processes():
    """Kills all persistent processes.

    On Windows systems the work directories are reserved by the ``Popen`` objects owned by the persistent managers.
    They need to be killed to before the directories can be modified, e.g. deleted or renamed.
    """
    _persistent_manager_factory.kill_manager_processes()


def enable_persistent_process_creation():
    """Enables creation of new persistent project managers."""
    _persistent_manager_factory.open_factory()


def disable_persistent_process_creation():
    """Disables creation of new persistent project managers."""
    _persistent_manager_factory.close_factory()


def issue_persistent_command(key, cmd):
    """See _PersistentManagerFactory."""
    yield from _persistent_manager_factory.issue_persistent_command(key, cmd)


def is_persistent_command_complete(key, cmd):
    """See _PersistentManagerFactory."""
    return _persistent_manager_factory.is_persistent_command_complete(key, cmd)


def get_persistent_completions(key, text):
    """See _PersistentManagerFactory."""
    return _persistent_manager_factory.get_persistent_completions(key, text)


def get_persistent_history_item(key, text, prefix, backwards):
    """See _PersistentManagerFactory."""
    return _persistent_manager_factory.get_persistent_history_item(key, text, prefix, backwards)


@contextlib.contextmanager
def acquire_persistent_process(group_persistent_managers, isolated_persistent_managers, running):
    while running and not persistent_process_semaphore.acquire(timeout=0.5):
        if not running:
            break
        killed = False
        for pm in group_persistent_managers.values():
            if pm.is_persistent_alive() and not pm.is_running_until_completion():
                pm.kill_process()
                killed = True
                break
        if not killed:
            for i, pm in enumerate(isolated_persistent_managers):
                if pm.is_persistent_alive() and pm.is_running_until_completion():
                    pm.kill_process()
                    isolated_persistent_managers.pop(i)
                    break
    yield None


class PersistentExecutionManagerBase(ExecutionManagerBase):
    """Base class for managing execution of commands on a persistent process."""

    def __init__(self, logger, args, commands, alias, group_id=None):
        """
        Args:
            logger (LoggerInterface): a logger instance
            args (list): List of args to start the persistent process
            commands (list): List of commands to execute in the persistent process
            alias (str): an alias name for the manager
            group_id (str, optional): item group that will execute using this kernel
        """
        super().__init__(logger)
        self._args = args
        self._commands = commands
        self._alias = alias
        self._persistent_manager = _persistent_manager_factory.new_persistent_manager(
            self.persistent_manager_factory, logger, args, group_id
        )

    @property
    def alias(self):
        return self._alias

    @property
    def persistent_manager_factory(self):
        """Returns a function to create a persistent manager for this execution
        (a subclass of PersistentManagerBase)

        Returns:
            Callable
        """
        raise NotImplementedError()

    def run_until_complete(self):
        """See base class."""
        if self._persistent_manager is None:
            return -1
        self._persistent_manager.set_running_until_completion(True)
        try:
            msg = dict(type="execution_started", args=" ".join(self._args))
            self._logger.msg_persistent_execution.emit(msg)
            fmt_alias = "# Running " + self._alias.rstrip()
            self._logger.msg_persistent_execution.emit(dict(type="stdin", data=fmt_alias))
            for cmd in self._commands:
                for msg in self._persistent_manager.issue_command(cmd):
                    if msg["type"] != "stdin":
                        self._logger.msg_persistent_execution.emit(msg)
                self.killed = not self._persistent_manager.is_persistent_alive()
                if not self._persistent_manager.command_successful:
                    return -1
            return 0
        finally:
            self._persistent_manager.set_running_until_completion(False)

    def stop_execution(self):
        """See base class."""
        if self._persistent_manager is not None:
            self._persistent_manager.interrupt_persistent()


class JuliaPersistentExecutionManager(PersistentExecutionManagerBase):
    """Manages execution of commands on a Julia persistent process."""

    @property
    def persistent_manager_factory(self):
        """See base class."""
        return JuliaPersistentManager


class PythonPersistentExecutionManager(PersistentExecutionManagerBase):
    """Manages execution of commands on a Python persistent process."""

    @property
    def persistent_manager_factory(self):
        """See base class."""
        return PythonPersistentManager
