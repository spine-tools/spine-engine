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

import socket

try:
    import readline
    import itertools
except ModuleNotFoundError:
    readline = None


def completions(text):
    if not readline:
        return None
    return " ".join(itertools.takewhile(bool, (readline.get_completer()(text, k) for k in range(100))))


def add_history(line):
    if not readline:
        return None
    readline.add_history(line)


def history_item(index):
    if not readline:
        return None
    return readline.get_history_item(readline.get_current_history_length() + 1 - index)


def send_msg(host, port, msg):
    if msg is None:
        msg = ""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(msg.encode("UTF8"))
