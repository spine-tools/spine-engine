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
Start script for Spine Engine Server.

"""

import sys
import time
from spine_engine.server.engine_server import EngineServer, ServerSecurityModel


def main(argv):
    """Spine Engine server main."""
    if len(argv) != 2 and len(argv) != 4:
        print(
            f"Spine Engine Server\n\nUsage:\n  python {argv[0]} <port>\n"
            f"or\n  python {argv[0]} <port> stonehouse <path_to_security_folder>\n"
            f"to enable security."
        )
        return
    server = None
    try:
        port = int(argv[1])
        if len(argv) == 2:
            server = EngineServer("tcp", port, ServerSecurityModel.NONE, "")
        elif len(argv) == 4:
            server = EngineServer("tcp", port, ServerSecurityModel.STONEHOUSE, argv[3])
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        return
    # Block main thread until user closes it
    print("Press c or Ctrl-c to close the server")
    print("\nListening...")
    kb_input = ""
    while kb_input != "c":
        try:
            kb_input = input()
        except (KeyboardInterrupt, EOFError):
            kb_input = "c"
        if kb_input == "c":
            try:
                server.close()
            except Exception as e:
                print(f"start_server.main(): {type(e).__name__}: {e}")
            break
        else:
            time.sleep(0.1)
    return


if __name__ == "__main__":
    main(sys.argv)
