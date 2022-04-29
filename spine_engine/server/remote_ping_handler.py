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
Handles remote pings received from the toolbox.
:authors: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   13.09.2021
"""

from spine_engine.server.util.server_message import ServerMessage


class RemotePingHandler:

    @staticmethod
    def handle_ping(connection):
        """Replies to a ping command.

        Args:
            connection (ZMQConnection): Zero-MQ connection
        """
        rq_id = connection.request_id()
        reply_msg = ServerMessage("ping", rq_id, "", None)
        reply_as_json = reply_msg.toJSON()
        reply_in_bytes = bytes(reply_as_json, "utf-8")
        connection.send_multipart_reply(reply_in_bytes)
        print(f"Replied to a Ping from {connection.connection_id()}")
