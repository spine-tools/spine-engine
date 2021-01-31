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
Contains the SpineEngineServer class.

:authors: M. Marin (KTH)
:date:   7.11.2020
"""

import socketserver
import threading
import json
import uuid
import atexit
from .spine_engine import SpineEngine
from .execution_managers import restart_kernel


class EngineRequestHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for our server.
    """

    _engines = {}
    _ENCODING = "ascii"

    def _run_engine(self, data):
        """
        Creates and engine and runs it.

        Args:
            data (dict): data to be passed as keyword arguments to SpineEngine()

        Returns:
            str: engine id, for further calls
        """
        engine_id = uuid.uuid4().hex
        self._engines[engine_id] = SpineEngine(**data, debug=False)
        return engine_id

    def _get_engine_event(self, engine_id):
        """
        Gets the next event in the engine's execution stream.

        Args:
            engine_id (str): the engine id, must have been returned by run.

        Returns:
            tuple(str,dict): two element tuple: event type identifier string, and event data dictionary
        """
        engine = self._engines[engine_id]
        event = engine.get_event()
        if event[0] == "dag_exec_finished":
            del self._engines[engine_id]
        return event

    def _stop_engine(self, engine_id):
        """
        Stops the engine.

        Args:
            engine_id (str): the engine id, must have been returned by run.
        """
        self._engines[engine_id].stop()

    def _restart_kernel(self, connection_file):
        """
        Restarts the jupyter kernel associated to given connection file.

        Args:
            connection_file (str): path of connection file
        """
        restart_kernel(connection_file)

    def handle(self):
        data = self._recvall()
        request, args = json.loads(data)
        handler = {
            "run_engine": self._run_engine,
            "get_engine_event": self._get_engine_event,
            "stop_engine": self._stop_engine,
            "restart_kernel": self._restart_kernel,
        }.get(request)
        if handler is None:
            return
        response = handler(*args)
        if response:
            self.request.sendall(bytes(json.dumps(response), self._ENCODING))

    def _recvall(self):
        """
        Receives and returns all data in the request.

        Returns:
            str
        """
        BUFF_SIZE = 4096
        fragments = []
        while True:
            chunk = str(self.request.recv(BUFF_SIZE), self._ENCODING)
            fragments.append(chunk)
            if len(chunk) < BUFF_SIZE:
                break
        return "".join(fragments)


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
