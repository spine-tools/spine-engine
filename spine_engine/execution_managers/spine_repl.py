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

import code
import socket
import socketserver
import threading

try:
    import itertools
    import readline

    _history_offset = -1
    _history_saved_line = ""
except ModuleNotFoundError:
    readline = None


class SpineDBServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


class _RequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024).decode("UTF8")
        req_args_sep = "\u001f"  # Unit separator
        args_sep = "\u0091"  # Private Use 1
        request, args = data.split(req_args_sep)
        args = args.split(args_sep)
        handler = {
            "completions": completions,
            "add_history": add_history,
            "history_item": history_item,
            "is_complete": is_complete,
        }.get(request)
        if handler is None:
            return
        response = handler(*args)
        try:
            self.request.sendall(bytes(response, "UTF8"))
        except:
            pass


def completions(text):
    if not readline:
        return ""
    return " ".join(itertools.takewhile(bool, (readline.get_completer()(text, k) for k in range(100))))


def add_history(line):
    if not readline:
        return
    readline.add_history(line)


def history_item(text, prefix, sense):
    if not readline:
        return ""
    global _history_offset  # pylint: disable=global-statement
    global _history_saved_line  # pylint: disable=global-statement
    if _history_offset == -1:
        _history_saved_line = text
    step = 1 if sense == "backwards" else -1
    cur_len = readline.get_current_history_length()
    while -1 <= _history_offset + step < cur_len:
        _history_offset += step
        if _history_offset == -1:
            return _history_saved_line
        item = readline.get_history_item(cur_len - _history_offset)
        if item.startswith(prefix):
            return item
    return ""


def is_complete(cmd):
    try:
        if code.compile_command(cmd) is None:
            return "false"
    except (SyntaxError, OverflowError, ValueError):
        pass
    return "true"


def start_server(address):
    """
    Args:
        address (tuple(str,int)): Server address
    """
    server = SpineDBServer(address, _RequestHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()


_exception = [False]


def set_exception(value):
    _exception[0] = value


def ping(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(b"error" if _exception[0] else b"ok")
    global _history_offset  # pylint: disable=global-statement
    _history_offset = -1
