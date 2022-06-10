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
Contains helper static methods for converting event/data information to JSON-based format and back.
:authors: P. Pääkkönen (VTT)
:date:   27.08.2021
"""

import base64


class EventDataConverter:
    @staticmethod
    def convert(eventData):
        """Converts events+data into a JSON string

        Args:
            eventData (list(tuple)): List of tuples containing events and data

        Returns:
            str: JSON string
        """
        itemCount = len(eventData)
        i = 0
        retStr = "{\n"
        retStr += "    \"items\": [\n"
        for ed in eventData:
            # Encode data to Base64
            msgBytes = str(ed[1]).encode('ascii')
            base64Bytes = base64.b64encode(msgBytes)
            base64Data = base64Bytes.decode('ascii')
            retStr += "    {\n        \"event_type\": \"" + ed[0] + "\",\n"
            if (i + 1) < itemCount:
                retStr += "        \"data\": \"" + base64Data + "\"\n    },\n"
            else:
                retStr += "        \"data\": \"" + base64Data + "\"\n    }\n"
            i += 1
        retStr += "    ]\n"
        retStr += "}\n"
        return retStr

    @staticmethod
    def convertJSON(parsed_json, base64Data):
        """Converts base64 encoded events to plain text in a list of tuples.
        Tuples contain the event_type and the associated data.

        Args:
            parsed_json (dict): events+data in dictionary
            base64Data (bool): flag indicating, whether data is encoded into Base64

        Returns:
            (list(tuple)): List of tuples containing events and data
        """
        itemsList = parsed_json['items']
        retList = []
        for item in itemsList:
            if base64Data == False:
                retList.append((item['event_type'], item['data']))
            else:  # decode Base64
                base64_bytes = item['data'].encode('ascii')
                message_bytes = base64.b64decode(base64_bytes)
                decodedData = message_bytes.decode('ascii')
                retList.append((item['event_type'], decodedData))
        return retList
