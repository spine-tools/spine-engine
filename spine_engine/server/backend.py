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
Server backend, responsible for handling managing threads and processes for incoming commands.
:authors: P. Savolainen (VTT)
:date:   30.11.2021
"""

import time
import threading
import os
import pathlib
import uuid
import zmq
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.util.server_message_parser import ServerMessageParser
from spine_engine.server.util.file_extractor import FileExtractor
from spine_engine.server.util.event_data_converter import EventDataConverter
from spine_engine.server.remote_execution_handler import RemoteExecutionHandler
from spine_engine.server.remote_ping_handler import RemotePingHandler


class Backend:
    """Catches messages from frontend and makes a new thread or process
    for processing the request. After processing sends the reply back to
    client via the frontend."""
    def __init__(self, context):
        """
        Args:
            context (zmq.Context): Context.
        """
        self.context = context
        # threading.Thread.__init__(self, name="Backend", target=self._run)
        # self.start()

    def _run(self):
        """Creates a new thread for each received message."""
        # execStartTimeMs = round(time.time() * 1000.0)
        # Parse JSON message
        backend_socket = self.context.socket(zmq.DEALER)
        backend_socket.connect("inproc://backend")
        worker_ctrl_socket = self.context.socket(zmq.PAIR)
        worker_ctrl_socket.connect("inproc://worker_ctrl")
        poller = zmq.Poller()
        print(f"_execute(): thread: {threading.current_thread()}")
        poller.register(backend_socket, zmq.POLLIN)
        poller.register(worker_ctrl_socket, zmq.POLLIN)
        while True:
            socks = dict(poller.poll())
            if socks.get(backend_socket) == zmq.POLLIN:
                print("backend receiving message")
                ident, msg, zip_file = backend_socket.recv_multipart()
                # Make some rudimentary checks before sending the message for processing
                if not self.check_msg_integrity(message, frontend):
                    continue
                # Start a worker for processing the message
                server_msg = ServerMessageParser.parse(message.decode("utf-8"))
                cmd = server_msg.getCommand()
                if cmd == "ping":  # Handle pings
                    print("Handling ping request")
                    RemotePingHandler.handlePing(parsed_msg, conn)  # TODO: Process Ping in a thread too?
                elif cmd == "execute":  # Handle execute messages
                    # if not len(message[1:]) == 2:  # Skip first part of the message (identity added by DEALER)
                    #     print(f"Not enough parts in received msg. Should be 2.")
                    #     self.send_error_reply(frontend, "", "",
                    #                           f"Message should have two parts. len(message): {len(message)}")
                    #     continue
                    print("Handling execute request")
                    backend.send_multipart([ident, message, zip_file])
                else:  # Unknown command
                    print(f"Unknown command '{cmd}' received. Sending 'Unknown command' response'")
                    self.send_error_reply(frontend, "", "", "Unknown command '{cmd}'")
                    continue

                print(ident)
                # Do execution
                self.do_execution(ident, msg, zip_file, worker_socket)
                break
            if socks.get(worker_ctrl_socket) == zmq.POLLIN:
                print("Killing worker thread")
                # Kill worker thread
                break
        print("Closing worker sockets")
        backend_socket.close()
        worker_ctrl_socket.close()

    def check_msg_integrity(self, b_msg, frontend):
        """Checks received message for errors. Sends an error reply to client if necessary.

        Args:
            msg (list): Received message
            frontend (zmq.Socket): Socket for sending the error reply

        Returns:
            bool: True if msg looks fine, False otherwise
        """
        # msg[0]: identity, msg[1]: engine_data, msg{2]: zip-file
        if len(b_msg) <= 10:  # msg[0]  # TODO: What is this about?
            print(f"Received msg too small. len(msg[0]):{len(b_msg)}. msg:{b_msg}")
            json_err_msg = json.dumps("Sent msg too small?!")
            self.send_response(frontend, "", "", json_err_msg)
            return False
        try:
            msg = b_msg.decode("utf-8")
        except UnicodeDecodeError as e:
            print(f"Decoding received msg '{b_msg}' failed. \nUnicodeDecodeError: {e}")
            self.send_response(
                frontend, "", "", json.dumps(f"UnicodeDecodeError: {e}. - Malformed message sent to server.")
            )
            return False
        try:
            parsed_msg = ServerMessageParser.parse(msg)
        except Exception as e:
            print(f"Parsing received msg '{msg}' failed. \n{type(e).__name__}: {e}")
            self.send_error_reply(frontend, "", "", f"{type(e).__name__}: {e}. - Server failed in parsing the message.")
            return False
        return True

    @staticmethod
    def send_response(socket, cmd, msg_id, data):
        """Sends reply back to client. Used after execution to send the events to client."""
        reply_msg = ServerMessage(cmd, msg_id, data, None)
        reply_as_json = reply_msg.toJSON()
        reply_in_bytes = bytes(reply_as_json, "utf-8")
        socket.send(reply_in_bytes)

    @staticmethod
    def send_error_reply(socket, cmd, msg_id, msg):
        """Sends an error message to client. Given msg string must be converted
        to JSON str (done by json.dumps() below) or parsing the msg on client
        fails. Do not use \n in the reply because it's not allowed in JSON.

        Args:
            socket (zmq.Socket): Socket for sending the message
            cmd (str): Recognized commands are 'execute' or 'ping'
            msg_id (str): Request message id
            msg (str): Error message sent to client
        """
        err_msg_as_json = json.dumps(msg)
        reply_msg = ServerMessage(cmd, msg_id, err_msg_as_json, [])
        reply_as_json = reply_msg.toJSON()
        reply_in_bytes = bytes(reply_as_json, "utf-8")
        socket.send(reply_in_bytes)
        print("\nClient has been notified. Moving on...")
