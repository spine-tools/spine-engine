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
from pickle import FALSE

"""
Starts the Remote Spine Server
:author: P. Pääkkönen (VTT)
:date:   01.09.2021
"""

import sys
import threading
import time
from spine_engine.server.remote_connection_handler import RemoteConnectionHandler
from spine_engine.server.connectivity.zmq_server import ZMQServer
from spine_engine.server.connectivity.zmq_server_observer import ZMQServerObserver
from spine_engine.server.connectivity.zmq_connection import ZMQConnection
from spine_engine.server.connectivity.zmq_server import ZMQSecurityModelState
from spine_engine.server.util.server_message_parser import ServerMessageParser
from spine_engine.server.remote_ping_handler import RemotePingHandler


class RemoteSpineService(ZMQServerObserver, threading.Thread):
    def __init__(self, protocol, port, zmqSecModelState, secFolder):
        """
        Args: 
            protocol: Zero-MQ protocol (string)
            port: Zero-MQ port to listen at for incoming connections
            zmqSecModelState: Zero-MQ security model (None, StoneHouse)
            secFolder: folder, where security files are stored at (if security mode StoneHouse is used) 
        """
        self.serviceRunning = False
        self.zmqServer = ZMQServer(protocol, port, self, zmqSecModelState, secFolder)
        # print("RemoteSpineService() initialised with protocol %s, port %d, Zero-MQ security model: %s, and sec.folder: %s"%(protocol,port,zmqSecModelState,secFolder))
        threading.Thread.__init__(self)
        self.start()

    def receiveConnection(self, conn: ZMQConnection) -> None:
        # print("RemoteSpineService.receiveConnection()")

        try:
            startTimeMs = round(time.time() * 1000.0)  # debugging
            msgParts = conn.getMessageParts()
            # print("RemoteSpineService.receiveConnection() msg parts:")
            # print(msgParts)
            msgPart1 = msgParts[0].decode("utf-8")
            # print("RemoteSpineService.receiveConnection() msg part 1: %s"%msgPart1)
            parsedMsg = ServerMessageParser.parse(msgPart1)
            # print("RemoteSpineService.receiveConnection() parsed msg with command: %s"%parsedMsg.getCommand())

            # handle pings
            if parsedMsg.getCommand() == "ping":
                print("Ping command received")
                RemotePingHandler.handlePing(parsedMsg, conn)

            # handle project execution messages
            elif parsedMsg.getCommand() == "execute":
                print("Execute command received")
                self.conn = conn
                self.connHandler = RemoteConnectionHandler(self.conn)
                # print("RemoteSpineService.receiveConnection() RemoteConnectionHandler started.")
            stopTimeMs = round(time.time() * 1000.0)
            # print("RemoteSpineService.receiveConnection() msg processing time %d ms"%(stopTimeMs-startTimeMs))
        except Exception as e:
            print("RemoteSpineService.receiveConnection(): exception: %s" % e)

    def close(self):
        """Closes the service."""
        self.serviceRunning = False

    def run(self):
        self.serviceRunning = True
        while self.serviceRunning:
            user_input = input()
            if user_input == "c":
                self.serviceRunning = False
            else:
                time.sleep(0.5)
        self.zmqServer.close()


def main(argv):
    # print("cmd line arguments: %s"%argv)
    userInput = ""
    remoteSpineService = None

    if len(argv) < 4:
        print(
            "protocol, port, security model (None or StoneHouse) and security "
            "folder (required with security) are required as parameters"
        )
        return
    if len(argv) != 5 and argv[3] == 'StoneHouse':
        print("security folder(required with security) is also required as a parameter")
        return

    if argv[3].lower() != 'stonehouse' and argv[3].lower() != 'none':
        print("invalid security model, use None or StoneHouse.")
        return

    try:
        portInt = int(argv[2])

        if len(argv) == 4 and argv[3].lower() == 'none':
            remoteSpineService = RemoteSpineService(argv[1], portInt, ZMQSecurityModelState.NONE, "")

        elif len(argv) == 5 and argv[3].lower() == 'stonehouse':
            remoteSpineService = RemoteSpineService(argv[1], portInt, ZMQSecurityModelState.STONEHOUSE, argv[4])

    except Exception as e:
        print("RemoteSpineService() error: %s" % e)
        print("%s must be a int (now it is %s)" % (argv[2], type(int(argv[2]))))
        return


if __name__ == "__main__":
    main(sys.argv)
