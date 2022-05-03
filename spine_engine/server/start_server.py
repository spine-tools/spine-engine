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
Starts Spine Engine Server
:author: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   01.09.2021
"""

import sys
import threading
import time
from spine_engine.server.connectivity.zmq_server import ZMQServer
from spine_engine.server.connectivity.zmq_server import ZMQSecurityModelState


class RemoteSpineService(threading.Thread):
    """Class for keeping remote server alive."""
    def __init__(self, protocol, port, zmq_sec_model_state, sec_folder):
        """
        Args: 
            protocol (str): Zero-MQ protocol
            port (int): Zero-MQ port to listen at for incoming connections
            zmq_sec_model_state (ZMQSecurityModelState): Zero-MQ security model
            sec_folder (str): Folder where security files are stored at (if security mode StoneHouse is used)
        """
        super().__init__(name="RemoteSpineServiceThread")
        self.zmqServer = ZMQServer(protocol, port, zmq_sec_model_state, sec_folder)
        self.serviceRunning = True
        self.start()

    def close(self):
        """Closes the service."""
        self.serviceRunning = False
        try:
            self.zmqServer.close()
        except Exception as e:
            print(f"RemoteSpineService.close(): {type(e).__name__}: {e}")

    def run(self):
        """Keeps RemoteSpineService running."""
        while self.serviceRunning:
            time.sleep(0.01)


def main(argv):
    """Spine Engine server main function."""
    if len(argv) != 3 and len(argv) != 5:
        print(f"This is the start script for Spine Engine Server\nUsage:\n\tpython {argv[0]} <protocol> <port>\n\n"
              f"or\n\tpython {argv[0]} <protocol> <port> stonehouse <path_to_security_folder>\n"
              f"to enable security."
        )
        return
    service = None
    try:
        port = int(argv[2])
        if len(argv) == 3:
            service = RemoteSpineService(argv[1], port, ZMQSecurityModelState.NONE, "")
        elif len(argv) == 5:
            service = RemoteSpineService(argv[1], port, ZMQSecurityModelState.STONEHOUSE, argv[4])
    except Exception as e:
        print(f"start_server.main(): {type(e).__name__}: {e}")
        return
    # Start listening for keyboard input
    print("Press c or Ctrl-c to close the server")
    print("\nListening...")
    kb_input = ""
    while kb_input != "c":
        try:
            kb_input = input()
        except KeyboardInterrupt:
            kb_input = "c"
        if kb_input == "c":
            service.close()
            break
        else:
            time.sleep(0.1)


if __name__ == "__main__":
    main(sys.argv)
