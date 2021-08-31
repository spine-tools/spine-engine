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
This class implements a Zero-MQ socket connection received from the ZMQServer.
:author: P. Pääkkönen (VTT)
:date:   19.8.2021
"""


class ZMQConnection:
    """
    Implementation of a Zero-MQ socket connection.
    Data can be received and sent based on a request/reply pattern enabled by Zero-MQ 
    (see Zero-MQ docs at https://zguide.zeromq.org/).
    """


    def __init__(
        self,socket,msgParts
    ):
        """
        Args:
            socket: ZMQSocket for communication
            msgParts: received message parts
        """
        self._socket=socket
        self._msgParts=msgParts
        #self._closed=False
        #print("ZMQConnection(): parts:")
        #print(self._msgParts)


    """
    Provides Zero-MQ message parts as a list of binary data.
    Returns:
        a list of binary data.
    """
    def getMessageParts(self):
        return self._msgParts



    """
    Sends a reply message to the recipient.
    Args:
        binary data
    Returns:
    """
    def sendReply(self,data):        
        self._socket.send(data)


    #"""
    #Closes the connection.
    #"""
    #def close(self):
    #    self._socket.close()
    #    self._closed=True

    
    #"""
    #Indicates if the connection has been closed.
    #Returns: 
    #    Boolean
    #"""
    #def closed(self):
    #    self._closed

