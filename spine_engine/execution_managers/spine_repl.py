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

import socketserver
import socket
import threading
import code

try:
    import readline
    import itertools
except ModuleNotFoundError:
    readline = None


class SpineDBServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


class _RequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024).decode("UTF8")
        request, _, arg = data.partition(";;")
        handler = {
            "completions": completions,
            "add_history": add_history,
            "history_item": history_item,
            "is_complete": is_complete,
        }.get(request)
        if handler is None:
            return
        response = handler(arg)
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


def history_item(index):
    if readline:
        return ""
    index = int(index)
    return readline.get_history_item(readline.get_current_history_length() + 1 - index)


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


def send_sentinel(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(b"sentinel")
