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
Contains ZMQServer class for running a Zero-MQ server with Spine Engine.
:authors: P. Pääkkönen (VTT)
:date:   19.08.2021
"""

import zmq
from enum import unique, Enum
import threading
from ZMQConnection import ZMQConnection
import time


@unique
class ZMQServerState(Enum):
    RUNNING = 1
    STOPPED = 2


class ZMQServer(threading.Thread):
    """
    A server implementation for receiving connections(ZMQConnection) from the Spine Toolbox.
    """


    def __init__(
        self,protocol,port,zmqServerObserver
    ):
        """        
        Args:
            protocol: protocol to be used by the server.
            port: port to bind the server to
        """

        if zmqServerObserver==None:
            raise ValueError("Invalid input ZMQServer: zmqServerObserver")
        self._observer=zmqServerObserver
        #threading.Thread.__init__(self)
        #self.start()
        try:
            context = zmq.Context() 
            self._socket = context.socket(zmq.REP)
            ret=self._socket.bind(protocol+"://*:"+str(port))     
            self._zmqContext=context
        except:
            print("ZMQServer couldn't be started due to exception")
            self._state=ZMQServerState.STOPPED
            raise ValueError("Invalid input ZMQServer.")            

        #self._state=ZMQServerState.RUNNING
        #self._socket=socket
        self.port=port
        print("ZMQServer started with protocol %s to port %d"%(protocol,port))
        threading.Thread.__init__(self)
        self.start()
 
   
    def close(self):
        """
        Closes the server.
        Returns:
            On success: 0, otherwise -1
        """
        if self._state==ZMQServerState.RUNNING:
            ret=self._socket.close()
            self._zmqContext.term()
            print("ZMQServer closed at port %d"%self.port)
            self._state=ZMQServerState.STOPPED
            return 0

        else:
            print("ZMQServer is not running, cannot close.")
            return -1


    def run(self):
        self._state=ZMQServerState.RUNNING
        self._receive_data()


    def _receive_data(self):
        #"""
        #Receives data from the socket, and creates new connections.
        #"""
        print("ZMQServer._receive_data()")
        try:
            while self._state==ZMQServerState.RUNNING:
                #print("ZMQServer._receive_data(): Starting listening..")
                msg_parts=self._socket.recv_multipart()
                self._conn=ZMQConnection(self._socket,msg_parts)
                self._observer.receiveConnection(self._conn)
                #print("ZMQServer._receive_data(): Received multi-part data.")
                time.sleep(.001)
        except:
            print("ZMQServer._receive_data(): reading failed probably due to closing of the server")
            self._state==ZMQServerState.STOPPED  
        print("ZMQServer._receive_data(): out..")
