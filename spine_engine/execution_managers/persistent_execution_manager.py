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

"""
Contains PersistentManagerBase, PersistentExecutionManagerBase classes and subclasses,
as well as some convenience functions.
"""
from dataclasses import dataclass
from multiprocessing import Lock, Process
import os
from queue import Empty, Queue
import signal
import socket
import socketserver
from subprocess import PIPE, Popen
import sys
import threading
import time
import uuid
from ..utils.execution_resources import persistent_process_semaphore
from ..utils.helpers import Singleton
from .execution_manager_base import ExecutionManagerBase

if sys.platform == "win32":
    import ctypes
    from subprocess import CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW


class PersistentIsDead(Exception):
    pass


class PersistentManagerBase:
    def __init__(self, args, group_id):
        """
        Args:
            args (list): the arguments to launch the persistent process
        """
        self._args = args
        self._group_id = group_id
        self._server_address = None
        self._msg_queue = Queue()
        self.command_successful = False
        self._is_running_lock = Lock()
        self._is_running = True
        self._persistent_resources_release_lock = Lock()
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
    def args(self):
        return self._args

    @property
    def group_id(self):
        return self._group_id

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
            self.port = s.server_address[1]  # Let OS select a free port
        self._server_address = host, self.port
        self.command_successful = False
        self._persistent = Popen(self._args + self._init_args(), **self._kwargs)
        threading.Thread(target=self._log_stdout, daemon=True).start()
        threading.Thread(target=self._log_stderr, daemon=True).start()

    def _log_stdout(self):
        """Puts stdout from the process into the queue (it will be consumed by issue_command())."""
        try:
            for line in iter(self._persistent.stdout.readline, b""):
                data = line.decode("UTF8", "replace").rstrip()
                self._msg_queue.put(dict(type="stdout", data=data))
        except ValueError:
            pass

    def _log_stderr(self):
        """Puts stderr from the process into the queue (it will be consumed by issue_command())."""
        try:
            for line in iter(self._persistent.stderr.readline, b""):
                data = line.decode("UTF8", "replace").rstrip()
                self._msg_queue.put(dict(type="stderr", data=data))
        except ValueError:
            pass

    def make_complete_command(self, cmd):
        lines = cmd.splitlines()
        cmd = os.linesep.join(lines)
        if len(lines) == 1:
            cmd += os.linesep
        try:
            result = self._communicate("is_complete", cmd)
        except PersistentIsDead:
            return None
        if result.strip() != "true":
            return None
        return cmd

    def drain_queue(self):
        """Empties the message queue.

        Yields:
            dict: messages still in the queue
        """
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
            catch_exception (bool)

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
            add_history (bool): Whether to add the command to history
            catch_exception (bool): whether to catch and report exceptions in the REPL as errors
        """
        with self._lock:
            self._msg_queue.put({"type": "stdin", "data": cmd})
            self.command_successful = self._issue_command(cmd, catch_exception=catch_exception)
            if self.command_successful:
                if add_history:
                    try:
                        self._communicate("add_history", cmd, receive=False)
                    except PersistentIsDead:
                        return
                self.command_successful &= self._wait()
        self._msg_queue.put({"type": "command_finished"})

    def _issue_command(self, cmd, catch_exception=True):
        """Writes command to the process's stdin and flushes.

        Args:
            cmd (str): Command to pass to the persistent process
            catch_exception (bool):

        Returns:
            bool: True if command was issued successfully, False otherwise
        """
        if catch_exception:
            cmd = self._catch_exception_command(cmd) if cmd else ""
        cmd += os.linesep
        try:
            self._persistent.stdin.write(cmd.encode("UTF8"))
        except ValueError:
            return False
        try:
            self._persistent.stdin.flush()
            return True
        except (BrokenPipeError, OSError):
            return False

    def _wait(self):
        """Waits for the persistent process to become idle.

        This is implemented as follows:
            1. Run a socket server on the current process that waits for a ping.
            2. Issue a command to the persistent process that pings that server.
        The ping command will run on the persistent process when it becomes idle and we will catch that situation
        in the server.

        Returns:
            bool: True if persistent process finished successfully, False otherwise
        """
        queue = Queue()
        thread = threading.Thread(target=self._wait_ping, args=(queue,))
        thread.start()
        host, port = queue.get()  # This blocks until the server is listening
        ping = self._ping_command(host, port)
        if not self._issue_command(ping, catch_exception=False):
            # Ping failed because pipe is broken, i.e. kernel died
            thread.join()
            self._msg_queue.put({"type": "stdout", "data": "Kernel died (×_×)"})
            return False
        result = queue.get()  # This blocks until the server is done
        thread.join()
        if not self.is_persistent_alive():
            self._msg_queue.put({"type": "stdout", "data": "Kernel died (×_×)"})
            success = self._persistent.returncode == 0
            self._release_persistent_resources()
            return success
        return result == "ok"

    def _wait_ping(self, queue, timeout=1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(timeout)
            host = "127.0.0.1"
            while True:
                with socketserver.TCPServer((host, 0), None) as s:
                    port = s.server_address[1]
                try:
                    server.bind((host, port))
                    break
                except OSError:
                    # OSError: [Errno 98] Address already in use
                    time.sleep(0.02)
            server.listen()
            queue.put((host, port))
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
        if not self.is_persistent_alive():
            raise PersistentIsDead()
        req_args_sep = "\u001f"  # Unit separator
        args_sep = "\u0091"  # Private Use 1
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
        try:
            result = self._communicate("completions", text)
        except PersistentIsDead:
            return []
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
        try:
            return self._communicate("history_item", text, prefix, sense).strip()
        except PersistentIsDead:
            return ""

    def restart_persistent(self):
        """Restarts the persistent process.

        Yields:
            dict: messages still in the queue
        """
        if not self.is_persistent_alive():
            self._start_persistent()
            self._wait()
            return
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
        queue = Queue()
        thread = threading.Thread(target=self._do_interrupt_persistent, args=(queue,))
        thread.start()
        # Wait till the process becomes idle, then put None to the queue
        self._wait()
        queue.put(None)
        thread.join()

    def _do_interrupt_persistent(self, queue):
        persistent = self._persistent  # Make local copy; other threads may set self._persistent to None while sleeping.
        if persistent is None:
            return
        while True:
            if sys.platform == "win32":
                p = Process(target=_send_ctrl_c, args=(persistent.pid,))
                p.start()
                p.join()
            else:
                persistent.send_signal(signal.SIGINT)
            try:
                # Wait 2 seconds to get an item from the queue, which means the process has become idle...
                # Otherwise just keep interrupting it
                queue.get(timeout=2)
                break
            except Empty:
                pass
        self.set_running_until_completion(False)

    def is_persistent_alive(self):
        """Whether the persistent is still alive and ready to receive commands.

        Returns:
            bool
        """
        return self._persistent is not None and self._persistent.poll() is None

    def kill_process(self):
        if self._persistent is None:
            return
        self._persistent.kill()
        self._persistent.wait()
        self._release_persistent_resources()
        self.set_running_until_completion(False)

    def _release_persistent_resources(self):
        """Releases the resources of the persistent process."""
        with self._persistent_resources_release_lock:
            if self._persistent is not None:
                self._persistent.stdin.close()
                self._persistent.stdout.close()
                self._persistent.stderr.close()
                self._persistent = None
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
            # "--threads=auto",
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
        value: bool = True

        def __bool__(self):
            return self.value

    persistent_managers = {}
    """Maps keys to associated PersistentManagerBase instances."""
    _factory_open = OpenSign()
    _idle_manager_lock = threading.Lock()

    @staticmethod
    def _emit_persistent_started(logger, key, language):
        msg = dict(type="persistent_started", key=key, language=language)
        logger.msg_persistent_execution.emit(msg)

    def _get_idle_persistent_managers(self):
        return [
            (key, pm)
            for key, pm in self.persistent_managers.items()
            if pm.is_persistent_alive() and not pm.is_running_until_completion()
        ]

    def _reuse_persistent_manager(self, idle_pms, logger, args, group_id):
        for key, pm in idle_pms:
            if pm.args == args and pm.group_id == group_id:
                logger.msg_warning.emit(f"Reusing process for group '{group_id}'")
                self._emit_persistent_started(logger, key, pm.language)
                return pm

    def new_persistent_manager(self, constructor, logger, args, group_id):
        """Creates a new persistent manager.

        Args:
            constructor (Callable): the persistent manager constructor
            logger (LoggerInterface)
            args (list): the arguments to launch the persistent process
            group_id (str): Item group for sharing this persistent process

        Returns:
            PersistentManagerBase: persistent manager or None if factory has been closed
        """
        while not persistent_process_semaphore.acquire(timeout=0.5):
            if not self._factory_open:
                return None
            with self._idle_manager_lock:
                idle_pms = self._get_idle_persistent_managers()
                # Try to reuse
                pm = self._reuse_persistent_manager(idle_pms, logger, args, group_id)
                if pm:
                    return pm
                # Kill an idle pm if any
                if idle_pms:
                    key, pm = idle_pms[0]
                    pm.kill_process()
                    del self.persistent_managers[key]
        # We got permission to create a new process
        # Try to reuse one last time just in case things changed quickly
        idle_pms = self._get_idle_persistent_managers()
        pm = self._reuse_persistent_manager(idle_pms, logger, args, group_id)
        if pm:
            return pm
        # No luck, just create the new process
        key = uuid.uuid4().hex
        try:
            pm = self.persistent_managers[key] = constructor(args, group_id)
        except OSError as err:
            msg = dict(type="persistent_failed_to_start", args=" ".join(args), error=str(err))
            logger.msg_persistent_execution.emit(msg)
            return None
        self._emit_persistent_started(logger, key, pm.language)
        for msg in pm.drain_queue():
            logger.msg_persistent_execution.emit(msg)
        return pm

    def restart_persistent(self, key):
        """Restart a persistent process.

        Args:
            key (tuple): persistent identifier

        Yields:
            dict: stdout and stderr messages (dictionaries with two keys: type, and data)
        """
        pm = self.persistent_managers.get(key)
        if pm is None:
            return
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

    def kill_persistent(self, key):
        """Kills a persistent process.

        Args:
            key (tuple): persistent identifier
        """
        pm = self.persistent_managers.get(key)
        if pm is None:
            return
        pm.kill_process()

    def issue_persistent_command(self, key, cmd):
        """Issues a command to a persistent process.

        Args:
            key (tuple): persistent identifier
            cmd (str): command to issue

        Yields:
            dict: stdin, stdout, and stderr messages (dictionaries with two keys: type, and data)
        """
        pm = self.persistent_managers.get(key)
        if pm is None:
            return
        yield from pm.issue_command(cmd, add_history=True, catch_exception=False)

    def is_persistent_command_complete(self, key, cmd):
        """Checks whether a command is complete.

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
            return ""
        return pm.get_history_item(text, prefix, backwards)

    def kill_manager_processes(self):
        """Kills persistent managers' Popen instances."""
        for manager in self.persistent_managers.values():
            manager.kill_process()

    def open_factory(self):
        """Opens factory.

        An open factory instantiates new persistent managers.
        """
        self._factory_open.value = True

    def close_factory(self):
        """Closes the factory.

        A closed factory does not instantiate new persistent managers
        """
        self._factory_open.value = False


_persistent_manager_factory = _PersistentManagerFactory()


def restart_persistent(key):
    """See _PersistentManagerFactory."""
    yield from _persistent_manager_factory.restart_persistent(key)


def interrupt_persistent(key):
    """See _PersistentManagerFactory."""
    _persistent_manager_factory.interrupt_persistent(key)


def kill_persistent(key):
    """See _PersistentManagerFactory."""
    _persistent_manager_factory.kill_persistent(key)


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


class PersistentExecutionManagerBase(ExecutionManagerBase):
    """Base class for managing execution of commands on a persistent process."""

    def __init__(self, logger, args, commands, alias, kill_completed_processes, group_id=None):
        """
        Args:
            logger (LoggerInterface): Logger instance
            args (list): Program and cmd line args for the persistent process
            commands (list): List of commands to execute in the persistent process (including the script cmd line args)
            alias (str): Alias name for the manager
            kill_completed_processes (bool): If True, the persistent process will be killed after execution
            group_id (str, optional): Item group for sharing this persistent process
        """
        super().__init__(logger)
        self._args = args
        self._commands = commands
        self._alias = alias
        self._persistent_manager = _persistent_manager_factory.new_persistent_manager(
            self.persistent_manager_factory, logger, args, group_id
        )
        self._kill_completed = kill_completed_processes

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
            if self._kill_completed:
                self._persistent_manager.kill_process()
                self._logger.msg_persistent_execution.emit({"type": "persistent_killed"})

    def stop_execution(self):
        """See base class."""
        if self._persistent_manager is not None:
            self._persistent_manager.interrupt_persistent()
            if self._kill_completed:
                self._persistent_manager.kill_process()
                self._logger.msg_persistent_execution.emit({"type": "persistent_killed"})


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
