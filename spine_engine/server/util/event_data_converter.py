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

"""Contains static methods for converting event and data information to JSON format and back."""
import base64
import json
from spine_engine.spine_engine import ItemExecutionFinishState
from spine_engine.utils.helpers import ExecutionDirection


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
        dict or str: Edited data dictionary or data string as it was.
    """
    if type(data) != str:
        if "item_state" in data.keys():
            data["item_state"] = str(data["item_state"])  # Cast ItemExecutionFinishState instance to string
        if "url" in data.keys():
            data["url"] = str(data["url"])  # Cast URL instances to string
        if "connection_file" in data.keys():
            # When the item requires a Jupyter Console for execution, this is the message that the client uses
            # to connect to the kernel manager running on server
            # kernel_execution_msg: {'item_name': 'T2', 'filter_id': '', 'type': 'kernel_started',
            #                        'connection_file': '/tmp/tmp_ve9ohel.json', 'kernel_name': 'python3'}
            # We need to read the connection file to a JSON dictionary and insert that to data as a new key
            with open(data["connection_file"], "r") as fh:
                try:
                    connection_file_dict = json.load(fh)
                    data["connection_file_dict"] = connection_file_dict
                except json.decoder.JSONDecodeError:
                    print(f"Error loading connection file {data['connection_file']}. Invalid JSON.")
        if "direction" in data.keys():
            data["direction"] = str(data["direction"])  # Cast ExecutionDirection instance to string
        for key in data.keys():
            # Print warning if there are any tuples used as keys in the data dictionary.
            # Tuples are converted to lists by json.dumps(). Lists must be converted back to tuples
            # on client side (in fix_event_data()).
            if type(data[key]) == tuple:
                print(f"[WARNING] Found tuple in message {event_type}: {data}. Fix this on client side.")
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
    if "direction" in event[1].keys():
        event[1]["direction"] = convert_execution_direction(event[1]["direction"])
    return event


def convert_execution_finish_state(state):
    """Transforms state string into an ItemExecutionFinishState enum.

    Args:
        state (str): State as string

    Returns:
        ItemExecutionFinishState: Enum if given str is valid, None otherwise.
    """
    states = {
        "SUCCESS": ItemExecutionFinishState.SUCCESS,
        "FAILURE": ItemExecutionFinishState.FAILURE,
        "SKIPPED": ItemExecutionFinishState.SKIPPED,
        "EXCLUDED": ItemExecutionFinishState.EXCLUDED,
        "STOPPED": ItemExecutionFinishState.STOPPED,
        "NEVER_FINISHED": ItemExecutionFinishState.NEVER_FINISHED,
    }
    return states.get(state, None)


def convert_execution_direction(direction):
    """Transforms direction string into an ExecutionDirection enum.

    Args:
        direction (str): Direction as string

    Returns:
        ExecutionDirection: Enum if given str is valid, None otherwise.
    """
    directions = {
        "FORWARD": ExecutionDirection.FORWARD,
        "BACKWARD": ExecutionDirection.BACKWARD,
        "NONE": ExecutionDirection.NONE,
    }
    return directions.get(direction, None)
