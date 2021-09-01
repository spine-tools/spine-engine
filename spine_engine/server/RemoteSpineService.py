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
Starts the Remote Spine Server
:author: P. Pääkkönen (VTT)
:date:   01.09.2021
"""

import unittest

import sys
sys.path.append('./connectivity')
sys.path.append('./util')
import zmq

from RemoteConnectionHandler import RemoteConnectionHandler
from ZMQServer import ZMQServer
from ZMQServerObserver import ZMQServerObserver
from ZMQConnection import ZMQConnection


class RemoteSpineService(ZMQServerObserver):

    def __init__(self,protocol,port):
        self.zmqServer=ZMQServer(protocol,port,self)
        print("RemoteSpineService() initialised with protocol %s and port %d"%(protocol,port))

    def receiveConnection(self,conn:ZMQConnection)-> None:
        print("RemoteSpineService.receiveConnection()")
        #parts=conn.getMessageParts()
        #print("TestObserver.receiveConnection(): parts received:")
        #print(parts)
        #conn.sendReply(conn.getMessageParts()[0])
        self.conn=conn
        self.connHandler=RemoteConnectionHandler(self.conn)
        print("RemoteSpineService.receiveConnection() RemoteConnectionHandler started.")

    #def getConnection(self):
    #    return self.conn




def main(argv):
    print("cmd line arguments: %s"%argv)
   
    if len(argv)!=3:
        print("protocol and port are required as parameters")
        return

    try:
        portInt=int(argv[2])
        RemoteSpineService(argv[1],portInt)

    except:
        print("%s must be a int (now it is %s)"%(argv[2],type(argv[2])))
        return
    

if __name__ == "__main__":
    main(sys.argv)





