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
Parser for JSON-based messages exchanged between server and clients.
:authors: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   25.08.2021
"""

import json
from spine_engine.server.util.server_message import ServerMessage


class ServerMessageParser:
    @staticmethod
    def parse(message):
        """Parses received message.

        Args:
            message (str): JSON message

        Returns:
            ServerMessage: Parsed message
        """
        parsed_msg = json.loads(message)  # Load JSON string into dictionary
        filenames = parsed_msg["files"]  # dict
        data = parsed_msg["data"]  # list
        parsed_filenames = list()
        if len(filenames) > 0:
            for f in filenames:
                parsed_filenames.append(filenames[f])
            msg = ServerMessage(parsed_msg['command'], parsed_msg['id'], data, parsed_filenames)
        else:
            msg = ServerMessage(parsed_msg['command'], parsed_msg['id'], data, None)
        return msg
