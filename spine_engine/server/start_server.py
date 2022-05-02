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
            protocol: Zero-MQ protocol (string)
            port: Zero-MQ port to listen at for incoming connections
            zmq_sec_model_state: Zero-MQ security model (None, StoneHouse)
            sec_folder: Folder, where security files are stored at (if security mode StoneHouse is used)
        """
        self.zmqServer = ZMQServer(protocol, port, self, zmq_sec_model_state, sec_folder)
        self.serviceRunning = True
        threading.Thread.__init__(self, name="RemoteSpineService")
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
    if len(argv) < 4:
        print(
            "protocol, port, security model (None or StoneHouse) and security "
            "folder (required with security) are required as parameters"
        )
        return
    if len(argv) != 5 and argv[3].lower() == "stonehouse":
        print("security folder (required with security) is also required as a parameter")
        return
    if argv[3].lower() != "stonehouse" and argv[3].lower() != "none":
        print("invalid security model, use None or StoneHouse")
        return
    service = None
    try:
        port = int(argv[2])
        if len(argv) == 4 and argv[3].lower() == "none":
            service = RemoteSpineService(argv[1], port, ZMQSecurityModelState.NONE, "")
        elif len(argv) == 5 and argv[3].lower() == "stonehouse":
            service = RemoteSpineService(argv[1], port, ZMQSecurityModelState.STONEHOUSE, argv[4])
        else:
            return
    except Exception as e:
        print(f"start_server.main(): {type(e).__name__}: {e}")
    # Start listening for keyboard input
    print("Press c to close the server")
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
