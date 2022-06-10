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
:author: P. Savolainen (VTT)
:date:   01.09.2021
"""

import sys
import time
from spine_engine.server.zmq_server import ZMQServer, ZMQSecurityModelState


def main(argv):
    """Spine Engine server main."""
    if len(argv) != 3 and len(argv) != 5:
        print(f"Spine Engine Server\n\nUsage:\n  python {argv[0]} <protocol> <port>\n"
              f"or\n  python {argv[0]} <protocol> <port> stonehouse <path_to_security_folder>\n"
              f"to enable security."
        )
        return
    zmq_server = None
    try:
        port = int(argv[2])
        if len(argv) == 3:
            zmq_server = ZMQServer(argv[1], port, ZMQSecurityModelState.NONE, "")
        elif len(argv) == 5:
            zmq_server = ZMQServer(argv[1], port, ZMQSecurityModelState.STONEHOUSE, argv[4])
    except Exception as e:
        print(f"start_server.main(): {type(e).__name__}: {e}")
        return
    # Block main thread until user closes it
    print("Press c or Ctrl-c to close the server")
    print("\nListening...")
    kb_input = ""
    while kb_input != "c":
        try:
            kb_input = input()
        except KeyboardInterrupt:
            kb_input = "c"
        if kb_input == "c":
            try:
                zmq_server.close()
            except Exception as e:
                print(f"ZMQServer.close(): {type(e).__name__}: {e}")
            break
        else:
            time.sleep(0.1)


if __name__ == "__main__":
    main(sys.argv)
