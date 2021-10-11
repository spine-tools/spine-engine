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
#from spine_server.server.connectivity.ZMQConnection import ZMQConnection
from .ZMQConnection import ZMQConnection
import time
import os
import ipaddress
import sys
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator

@unique
class ZMQServerState(Enum):
    RUNNING = 1
    STOPPED = 2

@unique
class ZMQSecurityModelState(Enum):
    NONE = 0       #ZMQ can be executed without security
    STONEHOUSE = 1 #stonehouse-security model of Zero-MQ


class ZMQServer(threading.Thread):
    """
    A server implementation for receiving connections(ZMQConnection) from the Spine Toolbox.
    """


    def __init__(
        self,protocol,port,zmqServerObserver,secModel,secFolder
    ):
        """
        Initialises the server.
        Args:
            protocol: protocol to be used by the server.
            port: port to bind the server to
            secModel: see: ZMQSecurityModelState 
            secFolder: folder, where security files have been stored.
        """

        if zmqServerObserver==None:
            raise ValueError("Invalid input ZMQServer: zmqServerObserver")
        self._observer=zmqServerObserver
        #threading.Thread.__init__(self)
        #self.start()
        try:
            if secModel==ZMQSecurityModelState.NONE:
                context = zmq.Context() 
                self._socket = context.socket(zmq.REP)
                #self._socket.setsockopt(zmq.LINGER, 1) #shutdown lingering
                ret=self._socket.bind(protocol+"://*:"+str(port))     
                self._zmqContext=context
                self._secModelState=ZMQSecurityModelState.NONE
            elif secModel==ZMQSecurityModelState.STONEHOUSE:
                #implementation based on https://github.com/zeromq/pyzmq/blob/main/examples/security/stonehouse.py
                if secFolder==None:
                    raise ValueError("ZMQServer(): security folder input is missing.")

                if len(secFolder)==0:
                    raise ValueError("ZMQServer(): security folder input is missing.")

                #print("beginning to configure security for stonehouse-model of ZMQ")
                base_dir = secFolder
                keys_dir = os.path.join(base_dir, 'certificates')
                public_keys_dir = os.path.join(base_dir, 'public_keys')
                secret_keys_dir = os.path.join(base_dir, 'private_keys')

                if not (
                    os.path.exists(keys_dir)
                    and os.path.exists(public_keys_dir)
                    and os.path.exists(secret_keys_dir)
                ):
                    raise ValueError("invalid certificate folders at ZMQServer()")
                
                self._zmqContext = zmq.Context.instance()

                # Start an authenticator for this context.
                self._auth = ThreadAuthenticator(self._zmqContext)
                self._auth.start()
                endpoints=self._readEndpoints(secFolder+"/allowEndpoints.txt")  #read endpoints to allow
                #print("read allowed endpoins from config file: %s"%endpoints)

                if len(endpoints)==0:
                    self._state=ZMQServerState.STOPPED
                    raise ValueError("Invalid input in allowEndpoints.txt at ZMQServer()")
                #allow configured endpoints
                for ep in endpoints:
                    try:
                        ipaddress.ip_address(ep.strip())
                        self._auth.allow(ep.strip()) 
                        #print("ZMQServer(): allowed endpoint %s"%ep.strip())
                    except:
                        print("ZMQServer(): invalid IP address: %s"%ep)
                #print("ZMQServer(): started authenticator.")

                # Tell the authenticator how to handle CURVE requests
                self._auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)

                self._socket =  self._zmqContext.socket(zmq.REP)
                server_secret_file = os.path.join(secret_keys_dir, "server.key_secret")
                server_public, server_secret = zmq.auth.load_certificate(server_secret_file)
                self._socket.curve_secretkey = server_secret
                self._socket.curve_publickey = server_public
                self._socket.curve_server = True  # must come before bind
                #print("ZMQServer(): binding server and listening (with stonehouse security configured)..")
                self._socket.bind(protocol+"://*:"+str(port))
                self._secModelState=ZMQSecurityModelState.STONEHOUSE

        except Exception as e:
            #print("ZMQServer couldn't be started due to exception: %s"%e)
            self._state=ZMQServerState.STOPPED
            raise ValueError("Invalid input ZMQServer.")            

        #self._state=ZMQServerState.RUNNING
        #self._socket=socket
        self.port=port
        #print("ZMQServer started with protocol %s to port %d"%(protocol,port))
        threading.Thread.__init__(self)
        #self.setDaemon(True)
        self.start()
 
   
    def close(self):
        """
        Closes the server.
        Returns:
            On success: 0, otherwise -1
        """
        if self._state==ZMQServerState.RUNNING:

            if self._secModelState==ZMQSecurityModelState.STONEHOUSE:
                self._auth.stop()
                #print("ZMQServer.close(): stopped security authenticator.")

            ret=self._socket.close()
            #print("ZMQServer.close(): socket closed.")
            #time.sleep(1)
            self._zmqContext.term()
            #self._zmqContext.destroy()
            #print("ZMQServer.close(): ZMQ context closed at port %d"%self.port)
            self._state=ZMQServerState.STOPPED
            return 0

        else:
            print("ZMQServer is not running (port %d), cannot close."%self.port)
            return -1


    def run(self):
        self._state=ZMQServerState.RUNNING
        self._receive_data()


    def _receive_data(self):
        #"""
        #Receives data from the socket, and creates new connections.
        #"""
        #print("ZMQServer._receive_data()")
        #try:
        while self._state==ZMQServerState.RUNNING:
            try:
                #print("ZMQServer._receive_data(): Starting listening..")
                msg_parts=self._socket.recv_multipart()
                self._conn=ZMQConnection(self._socket,msg_parts)
                self._observer.receiveConnection(self._conn)
                #print("ZMQServer._receive_data(): Received multi-part data.")
                time.sleep(0.01)
            except Exception as e: 
                #print("ZMQServer._receive_data(): reading failed, exception: %s"%e)            
                time.sleep(0.01)
                if str(e)=="Context was terminated":
                    print("ZMQServer._receive_data(): ZMQ context was terminated,out..")
                    self._state=ZMQServerState.STOPPED
                    #return
                #self._state==ZMQServerState.STOPPED  
        #print("ZMQServer._receive_data(): out..")



    def _readEndpoints(self,configFileLocation):
       
        with open(configFileLocation) as f:
            lines = f.readlines()
            return lines
