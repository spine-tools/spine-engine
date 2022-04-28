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
:authors: P. Pääkkönen (VTT)
:date:   25.08.2021
"""

import json
from spine_engine.server.util.server_message import ServerMessage


class ServerMessageParser:
    @staticmethod
    def parse(message):
        """Parses received message

        Args:
            message: JSON-message as a string

        Returns:
            ServerMessage: Parsed message
        """
        if not message:
            raise ValueError("invalid input to ServerMessageParser.parse()")
        # Load JSON string into dictionary
        parsedMsg = json.loads(message)
        fileNames = parsedMsg['files']
        dataStr = parsedMsg['data']
        parsedFileNames = []
        if len(fileNames) > 0:
            for f in fileNames:
                parsedFileNames.append(fileNames[f])
            msg = ServerMessage(parsedMsg['command'], parsedMsg['id'], dataStr, parsedFileNames)
        else:
            msg = ServerMessage(parsedMsg['command'], parsedMsg['id'], dataStr, None)
        return msg
