:py:mod:`spine_engine.spine_engine_server`
==========================================

.. py:module:: spine_engine.spine_engine_server

.. autoapi-nested-parse::

   Contains the SpineEngineServer class.

   :authors: M. Marin (KTH)
   :date:   7.11.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.spine_engine_server.EngineRequestHandler
   spine_engine.spine_engine_server.SpineEngineServer



Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.spine_engine_server.start_spine_engine_server
   spine_engine.spine_engine_server._shutdown_servers



Attributes
~~~~~~~~~~

.. autoapisummary::

   spine_engine.spine_engine_server._servers


.. py:class:: EngineRequestHandler(request, client_address, server)

   Bases: :py:obj:`socketserver.BaseRequestHandler`

   The request handler class for our server.

   .. py:attribute:: _engines
      

      

   .. py:attribute:: _ENCODING
      :annotation: = ascii

      

   .. py:method:: _run_engine(self, data)

      Creates and engine and runs it.

      :param data: data to be passed as keyword arguments to SpineEngine()
      :type data: dict

      :returns: engine id, for further calls
      :rtype: str


   .. py:method:: _get_engine_event(self, engine_id)

      Gets the next event in the engine's execution stream.

      :param engine_id: the engine id, must have been returned by run.
      :type engine_id: str

      :returns: two element tuple: event type identifier string, and event data dictionary
      :rtype: tuple(str,dict)


   .. py:method:: _stop_engine(self, engine_id)

      Stops the engine.

      :param engine_id: the engine id, must have been returned by run.
      :type engine_id: str


   .. py:method:: _restart_kernel(self, connection_file)

      Restarts the jupyter kernel associated to given connection file.

      :param connection_file: path of connection file
      :type connection_file: str


   .. py:method:: _shutdown_kernel(self, connection_file)

      Shuts down the jupyter kernel associated to given connection file.

      :param connection_file: path of connection file
      :type connection_file: str


   .. py:method:: handle(self)


   .. py:method:: _recvall(self)

      Receives and returns all data in the request.

      :returns: str



.. py:class:: SpineEngineServer(server_address, RequestHandlerClass, bind_and_activate=True)

   Bases: :py:obj:`socketserver.ThreadingMixIn`, :py:obj:`socketserver.TCPServer`

   Mix-in class to handle each request in a new thread.

   Constructor.  May be extended, do not override.

   .. py:attribute:: allow_reuse_address
      :annotation: = True

      


.. py:data:: _servers
   :annotation: = []

   

.. py:function:: start_spine_engine_server(host, port)


.. py:function:: _shutdown_servers()


