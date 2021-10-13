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
Handles remote pings received from the toolbox.
:authors: P. Pääkkönen (VTT)
:date:   13.09.2021
"""

from spine_engine.server.util.ServerMessage import ServerMessage
from spine_engine.server.connectivity.ZMQConnection import ZMQConnection


class RemotePingHandler:

    @staticmethod
    def handlePing(pingMessage,zmqConnection):
        """
        Args:
            pingMessage: ServerMessage received (see util.ServerMessage) 
            zmqConnection: Zero-MQ connection (see connection.ZMQConnection)
        """
        replyMsg=ServerMessage("ping",pingMessage.getId(),"",None)
        replyAsJson=replyMsg.toJSON()
        replyInBytes= bytes(replyAsJson, 'utf-8')
        zmqConnection.sendReply(replyInBytes)
        #print("RemotePingHandler.handlePing() sent response: %s"%replyAsJson)   
