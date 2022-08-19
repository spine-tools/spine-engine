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
Contains a class for handling event queries in a dedicated worker thread.
:authors: P. Savolainen (VTT)
:date:   12.08.2022
"""

import threading
import queue
import zmq
from spine_engine.server.util.event_data_converter import EventDataConverter


class RemoteEventQueryHandler(threading.Thread):
    """Class for handling event queries requests."""
    def __init__(self, context, request, q, job_id):
        """Initializes instance.

        Args:
            context (zmq.Context): Server context
            request (Request): Client request
            q (Queue): Event Queue
            job_id (str): Worker thread Id
        """
        super().__init__(name="QueryHandlerThread")
        self.context = context
        self.req = request
        self.event_q = q
        self.job_id = job_id
        self.worker_socket = self.context.socket(zmq.DEALER)  # Backend socket

    def run(self):
        """Reads all events from a queue and sends them to frontend, from where they are relayed back to client."""
        self.worker_socket.connect("inproc://backend")
        events = list()
        query_msg = ""
        while True:
            try:
                events.append(self.event_q.get_nowait())
            except queue.Empty:
                break
        if len(events) > 0:
            # If the last event is "COMPLETED" or "FAILED", this queue has been depleted and should
            # be removed from references at frontend
            if events[-1][1] == "COMPLETED" or events[-1][1] == "FAILED":
                query_msg = "queue_exhausted_" + self.req.request_id()
            events = EventDataConverter.convert(events)
        self.req.send_response(self.worker_socket, events, (self.job_id, query_msg), dump_to_json=False)

    def close(self):
        """Closes socket and cleans up."""
        self.worker_socket.close()
