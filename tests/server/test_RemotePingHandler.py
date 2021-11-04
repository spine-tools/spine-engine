#####################################################################################################################
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
Unit tests for RemotePingHandler class.
:author: P. Pääkkönen (VTT)
:date:   13.09.2021
"""

import unittest
import zmq
import time
from spine_engine.server.util.server_message import ServerMessage
from spine_engine.server.start_server import RemoteSpineService
from spine_engine.server.connectivity.zmq_server import ZMQSecurityModelState


class TestRemotePingHandler(unittest.TestCase):
    def test_ping_tcp(self):
        """Tests starting of a ZMQ server with tcp, and pinging it."""
        remoteSpineService = RemoteSpineService("tcp", 7000, ZMQSecurityModelState.NONE, "")

        # connect to the server
        # msg_parts=[]
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://localhost:7000")
        # time.sleep(1)
        i = 0
        while i < 10:
            # startTimeMs=round(time.time()*1000.0)
            msg_parts = []
            pingMsg = ServerMessage("ping", str(i), "", None)
            pingAsJson = pingMsg.toJSON()
            pingInBytes = bytes(pingAsJson, 'utf-8')
            msg_parts.append(pingInBytes)
            startTimeMs = round(time.time() * 1000.0)
            socket.send_multipart(msg_parts)
            # print("test_ping_tcp() msg parts sent.")
            # time.sleep(1)
            msg = socket.recv()
            stopTimeMs = round(time.time() * 1000.0)
            # print("test_ping_tcp() msg received: %s"%msg)
            msgStr = msg.decode("utf-8")
            # print("test_ping_tcp() msg received: %s"%msgStr)
            self.assertEqual(msgStr, pingAsJson)  # check that echoed content is as expected
            # time.sleep(0.1)
            # stopTimeMs=round(time.time()*1000.0)
            # print("test_ping_tcp(): ZMQ transfer time %d ms"%(stopTimeMs-startTimeMs))
            i = i + 1
        socket.close()
        remoteSpineService.close()
        context.term()

    def test_noconnection(self):
        """Tests connection failure at sending."""
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 1)
        socket.connect("tcp://localhost:7002")
        msg_parts = []
        pingMsg = ServerMessage("ping", "2", "", None)
        pingAsJson = pingMsg.toJSON()
        pingInBytes = bytes(pingAsJson, 'utf-8')
        msg_parts.append(pingInBytes)
        startTimeMs = round(time.time() * 1000.0)
        sendRet = socket.send_multipart(msg_parts, flags=zmq.NOBLOCK)
        # print("send ret: %s"%sendRet)
        event = socket.poll(timeout=1000)
        if event == 0:
            # print("test_noconnection(): timeout occurred, no reply will be listened to")
            pass
        else:
            msg = socket.recv()
            msgStr = msg.decode("utf-8")
            print("test_noconnection(): message was received :%s" % msgStr)
        self.assertEqual(event, 0)
        socket.close()
        context.term()


if __name__ == '__main__':
    unittest.main()
