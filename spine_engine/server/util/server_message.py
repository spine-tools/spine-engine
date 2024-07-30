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

"""
Contains a helper class for JSON-based messages exchanged between server and clients.
"""

import json


class ServerMessage:
    """Class for communicating requests and replies between the client and the server."""

    def __init__(self, command, req_id, data, files=None):
        """
        Supported requests and expected server responses
        - Ping
            request: ServerMessage("ping", id, "", None)
            response: ServerMessage("ping", id, "", None)
        - Prepare project execution
            request: ServerMessage("prepare_execution", "1", <project_name>, [project_package.zip]) + file as bytes
            response: ServerMessage("prepare_execution, job_id, "", None)
        - Start DAG execution
            request: ServerMessage("start_execution", job_id, engine_data, None)
            response: ServerMessage("start_execution", job_id, ("remote_execution_started", publish_port), [])
        - Retrieve finished project
            request: ServerMessage("retrieve_project", job_id, "", [])
            response ServerMessage("retrieve_project, job_id, "", ["project_package.zip"]) + file as bytes

        Args:
            command (str): Command to be executed at the server
            req_id (str): Identifier associated with the command
            data (str): Data associated to the command. In an execute request, this is the engine
            data as a JSON string. In an execute reply, this is an event_type:data (str:str) tuple.
            files (list[str], None): List of file names to be associated with the message (optional)
        """
        self._command = command
        self._id = req_id
        self._data = data
        if not files:
            self._files = list()
        else:
            self._files = files  # Name of the file where zip-file is saved to. Does not need to be the same as original

    def getCommand(self):
        return self._command

    def getId(self):
        return self._id

    def getData(self):
        return self._data

    def getFileNames(self):
        return self._files

    def toJSON(self):
        """Converts this instance into a JSON string.

        Returns:
            str: The instance as a JSON string
        """
        jsonFileNames = self._getJSONFileNames()
        retStr = ""
        retStr += "{\n"
        retStr += '   "command": "' + self._command + '",\n'
        retStr += '   "id":"' + self._id + '",\n'

        if len(self._data) == 0:
            retStr += '   "data":"",\n'
        else:
            retStr += '   "data":' + self._data + ",\n"
        retStr += '   "files": ' + jsonFileNames
        retStr += "}"
        return retStr

    def _getJSONFileNames(self):
        fileNameCount = len(self._files)
        if fileNameCount == 0:
            return "{}\n"
        retStr = "{\n"
        i = 0
        for fName in self._files:
            if i + 1 < fileNameCount:
                retStr = retStr + '    "name-' + str(i) + '": "' + fName + '",\n'
            else:
                retStr = retStr + '    "name-' + str(i) + '": "' + fName + '"\n'
            i += 1
        retStr = retStr + "    }\n"
        return retStr

    def to_bytes(self):
        """Converts this ServerMessage instance to a JSON and then to a bytes string.

        Returns:
            bytes: ServerMessage instance as a UTF-8 bytes JSON string.
        """
        as_json = self.toJSON()
        return bytes(as_json, "utf-8")

    @classmethod
    def parse(cls, message):
        """Makes a ServerMessage instance from a received bytes object.

        Args:
            message (bytes): JSON message as bytes

        Returns:
            ServerMessage: Parsed message
        """
        parsed_msg = json.loads(message.decode("utf-8"))  # Load JSON string into dictionary
        filenames = parsed_msg["files"]  # dict
        data = parsed_msg["data"]  # list
        parsed_filenames = list()
        if len(filenames) > 0:
            for f in filenames:
                parsed_filenames.append(filenames[f])
            msg = cls(parsed_msg["command"], parsed_msg["id"], data, parsed_filenames)
        else:
            msg = cls(parsed_msg["command"], parsed_msg["id"], data, None)
        return msg
