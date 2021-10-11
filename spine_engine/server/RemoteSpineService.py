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
#sys.path.append('./connectivity')
#sys.path.append('./util')
import zmq
import threading
import time

from RemoteConnectionHandler import RemoteConnectionHandler
from spine_engine.server.connectivity.ZMQServer import ZMQServer
from spine_engine.server.connectivity.ZMQServerObserver import ZMQServerObserver
from spine_engine.server.connectivity.ZMQConnection import ZMQConnection
from spine_engine.server.connectivity.ZMQServer import ZMQSecurityModelState


class RemoteSpineService(ZMQServerObserver,threading.Thread):

    def __init__(self,protocol,port,zmqSecModelState,secFolder):
        self.zmqServer=ZMQServer(protocol,port,self,zmqSecModelState,secFolder)
        #time.sleep(1)
        print("RemoteSpineService() initialised with protocol %s, port %d, Zero-MQ security model: %s, and sec.folder: %s"%(protocol,port,zmqSecModelState,secFolder))
        threading.Thread.__init__(self)
        self.start()


    def receiveConnection(self,conn:ZMQConnection)-> None:
        #print("RemoteSpineService.receiveConnection()")
        #parts=conn.getMessageParts()
        #print("TestObserver.receiveConnection(): parts received:")
        #print(parts)
        #conn.sendReply(conn.getMessageParts()[0])
        self.conn=conn
        self.connHandler=RemoteConnectionHandler(self.conn)
        #print("RemoteSpineService.receiveConnection() RemoteConnectionHandler started.")


    def run(self):
        #print("RemoteSpineService() press c to close the server")
        self.serviceRunning=True
               
        while self.serviceRunning==True:
            print("RemoteSpineService() press c to close the server")
            userInput=input()
            #print("RemoteSpineService() user input: %s"%userInput)
            if userInput=="c":
                self.zmqServer.close()
                print("RemoteSpineService() closed.")
                self.serviceRunning=False
            else:
                time.sleep(1)
        print("RemoteSpineService().run() .. out") 


def main(argv):
    #print("cmd line arguments: %s"%argv)
   
    if len(argv)<4:
        print("protocol, port, security model(None,StoneHouse) and security folder(required with security) are required as parameters")
        return
    if len(argv)!=5 and argv[3]=='StoneHouse':
        print("security folder(required with security) is also required as a parameter")
        return

    if argv[3]!='StoneHouse' and argv[3]!='None':
        print("invalid security model, use None or StoneHouse.")
        return

    try:
        portInt=int(argv[2])

        if len(argv)==4 and argv[3]=='None':
            RemoteSpineService(argv[1],portInt,ZMQSecurityModelState.NONE,"")

        elif len(argv)==5 and argv[3]=='StoneHouse':
            RemoteSpineService(argv[1],portInt,ZMQSecurityModelState.STONEHOUSE,argv[4])

    except Exception as e:
        print("RemoteSpineService() error: %s"%e)
        print("%s must be a int (now it is %s)"%(argv[2],type(int(argv[2]))))
        return
    

if __name__ == "__main__":
    main(sys.argv)





