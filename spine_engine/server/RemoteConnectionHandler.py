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
Contains RemoteConnectionHandler class for receiving control messages(with project content) from the Toolbox,
and running a DAG with Spine Engine. Only one DAG can be executed at a time.
:authors: P. Pääkkönen (VTT)
:date:   24.08.2021
"""

import threading
import sys

sys.path.append('./util')
from ServerMessageParser import ServerMessageParser
from ServerMessage import ServerMessage


class RemoteConnectionHandler(threading.Thread):

    """
    Handles one remote connection at a time from Spine Toolbox, executes a DAG, and returns 
    response to the client.
    """


    def __init__(
        self,zmqConnection
    ):
        """
        Args:
            zmqConnection: Zero-MQ connection of the client.
        """
        if zmqConnection==None:
            raise ValueError("No Zero-MQ connection was provided to RemoteConnectionHandler()")
        self.zmqConn=zmqConnection
        threading.Thread.__init__(self)
        self.start()


    def run(self):
        print("run()")
        self._execute()
        


    def _execute(self):
        """
        Executes a query with the Spine engine, and returns a response to the Zero-MQ client.
        """
        #get message parts sent by the client
        msgParts=self.zmqConn.getMessageParts()
        print("RemoteConnectionHandler._execute() Received: ")
        print(msgParts)

        #parse JSON message 
        msgPart1=str(msgParts[0])
        if len(msgPart1)>10:
            parsedMsg=ServerMessageParser.parse(msgParts)
            print("parsed msg with command: %d"%parsedMsg.getCommand()) 
        
