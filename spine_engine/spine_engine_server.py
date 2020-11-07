######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains the SpineEngineServer class.

:authors: M. Marin (KTH)
:date:   7.11.2020
"""

import socketserver
import threading
import json
import uuid
import atexit
from .spine_engine_experimental import SpineEngineExperimental
from .utils.helpers import recvall


class EngineRequestHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for our server.
    """

    _engines = {}

    def handle(self):
        data = recvall(self.request)
        head, body = json.loads(data)
        if head == "run":
            engine_id = uuid.uuid4().hex
            self._engines[engine_id] = SpineEngineExperimental(**body, debug=False)
            self.request.sendall(bytes(engine_id, "ascii"))
        elif head == "get_event":
            engine_id = body
            engine = self._engines[engine_id]
            event = engine.get_event()
            response = json.dumps(event)
            self.request.sendall(bytes(response, "ascii"))
            if event[0] == "dag_exec_finished":
                del self._engines[engine_id]
        elif head == "stop":
            engine_id = body
            engine = self._engines[engine_id]
            engine.stop()


class SpineEngineServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


_servers = []


def start_spine_engine_server(host, port):
    server = SpineEngineServer((host, port), EngineRequestHandler)
    _servers.append(server)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()


def _shutdown_servers():
    for server in _servers:
        server.shutdown()
        server.server_close()


atexit.register(_shutdown_servers)
