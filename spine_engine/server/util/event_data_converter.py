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
Contains static methods for converting event and data information to JSON format and back.
:authors: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   27.08.2021
"""

import base64
import json


class EventDataConverter:
    @staticmethod
    def convert(event_data):
        """Converts events and data into a JSON string on server side. Data is decoded as base64.

        Args:
            event_data (list(tuple)): List of tuples containing events and data

        Returns:
            str: JSON string
        """
        items_list = list()
        for t in event_data:
            msg_b = str(t[1]).encode("ascii")
            base64_b = base64.b64encode(msg_b)
            base64_data = base64_b.decode("ascii")
            items_list.append({"event_type": t[0], "data": base64_data})
        json_event_data = json.dumps({"items": items_list})
        return json_event_data

    @staticmethod
    def deconvert(event_data, base64Data):
        """Converts the received event+data dictionary at client side back to a list-of-tuples.
        Base64 encoded events are decoded to plain text if base64Data == True.

        Args:
            event_data (dict): Events and data in dictionary
            base64Data (bool): Flag indicating, whether data is encoded into Base64

        Returns:
            (list(tuple)): List of tuples containing event_type and the associated data
        """
        items_list = event_data["items"]
        ret_list = []
        for item in items_list:
            if not base64Data:
                ret_list.append((item["event_type"], item["data"]))
            else:  # Decode Base64
                base64_bytes = item["data"].encode("ascii")
                message_bytes = base64.b64decode(base64_bytes)
                decoded_data = message_bytes.decode("ascii")
                ret_list.append((item["event_type"], decoded_data))
        return ret_list
