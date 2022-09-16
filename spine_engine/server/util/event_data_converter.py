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
from spine_engine.spine_engine import ItemExecutionFinishState


class EventDataConverter:
    @staticmethod
    def convert(event_type, data, b64encoding=False):
        """Converts a single event_type, data pair into a JSON string.
         Optionally, encodes data as base64.

        Args:
            event_type: (str): Event type (e.g. exec_started, dag_exec_finished, etc.)
            data (dict or str): Data associated with the event_type
            b64encoding (bool): True encodes data as base64, False does not

        Returns:
            str: JSON string
        """
        if b64encoding:
            msg_b = str(data).encode("ascii")
            base64_b = base64.b64encode(msg_b)
            data = base64_b.decode("ascii")
        data = break_event_data(event_type, data)
        event_dict = {"event_type": event_type, "data": data}
        json_event_data = json.dumps(event_dict)
        return json_event_data

    @staticmethod
    def deconvert(event_data, b64decoding=False):
        """Decodes a bytes object into a JSON string, then converts it to a
        dictionary with a single event_type, data pair into a tuple
        containing the same. Optionally, decodes data field from base64
        back to ascii.

        Args:
            event_data (bytes): Event type and data as bytes
            b64decoding (bool): Flag indicating, whether data is decoded from base64

        Returns:
            (tuple): Event type and data pair
        """
        event_dict = json.loads(event_data.decode("utf-8"))
        if b64decoding:
            base64_bytes = event_dict["data"].encode("ascii")
            message_bytes = base64.b64decode(base64_bytes)
            data = message_bytes.decode("ascii")
        else:
            data = event_dict["data"]
        fixed_event = fix_event_data((event_dict["event_type"], data))
        return fixed_event


def break_event_data(event_type, data):
    """Makes values in data dictionary suitable for converting them to JSON strings.

    Args:
        event_type (str): Event type
        data (dict or str): Data

    Returns:
        dict or str: Converted data dictionary or data string as it was.
    """
    if type(data) != str:
        if "item_state" in data.keys():
            data["item_state"] = str(data["item_state"])  # Cast ItemExecutionFinishState instance to string
        if "url" in data.keys():
            data["url"] = str(data["url"])  # Cast URL instances to string
        for key in data.keys():
            if type(data[key]) == tuple:
                # tuples are converted to lists by json.dumps(). Convert the lists back to tuples on client side
                print(f"[DEBUG] Found tuple in {event_type}: data. This may be a problem on client side.")
    return data


def fix_event_data(event):
    """Does the opposite of break_event_data(). Converts values in data dictionary back to original.

    Args:
        event (tuple): (event_type, data). event_type is str, data is dict or str.

    Returns:
        tuple: Fixed event_type: data tuple
    """
    # Convert item_state str back to ItemExecutionFinishState. This was converted to str on server because
    # it is not JSON serializable
    if type(event[1]) == str:
        return event
    if "item_state" in event[1].keys():
        event[1]["item_state"] = convert_execution_finish_state(event[1]["item_state"])
    # Fix persistent console key. It was converted from tuple to a list by JSON.dumps but we need it as
    # a tuple because it will be used as dictionary key and lists cannot be used as keys
    if event[0] == "persistent_execution_msg" and "key" in event[1].keys():
        if type(event[1]["key"]) == list:
            event[1]["key"] = tuple(event[1]["key"])
    return event


def convert_execution_finish_state(state):
    """Transforms state string into an ItemExecutionFinishState enum.

    Args:
        state (str): State as string

    Returns:
        ItemExecutionFinishState: Enum if given str is valid, None otherwise.
    """
    states = dict()
    states["SUCCESS"] = ItemExecutionFinishState.SUCCESS
    states["FAILURE"] = ItemExecutionFinishState.FAILURE
    states["SKIPPED"] = ItemExecutionFinishState.SKIPPED
    states["EXCLUDED"] = ItemExecutionFinishState.EXCLUDED
    states["STOPPED"] = ItemExecutionFinishState.STOPPED
    states["NEVER_FINISHED"] = ItemExecutionFinishState.NEVER_FINISHED
    return states.get(state, None)
