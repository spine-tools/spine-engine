:py:mod:`spine_engine.execution_managers.spine_repl`
====================================================

.. py:module:: spine_engine.execution_managers.spine_repl


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.spine_repl.SpineDBServer
   spine_engine.execution_managers.spine_repl._RequestHandler



Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.spine_repl.completions
   spine_engine.execution_managers.spine_repl.add_history
   spine_engine.execution_managers.spine_repl.history_item
   spine_engine.execution_managers.spine_repl.is_complete
   spine_engine.execution_managers.spine_repl.start_server
   spine_engine.execution_managers.spine_repl.send_sentinel



Attributes
~~~~~~~~~~

.. autoapisummary::

   spine_engine.execution_managers.spine_repl.readline


.. py:data:: readline
   

   

.. py:class:: SpineDBServer(server_address, RequestHandlerClass, bind_and_activate=True)

   Bases: :py:obj:`socketserver.ThreadingMixIn`, :py:obj:`socketserver.TCPServer`

   Mix-in class to handle each request in a new thread.

   Constructor.  May be extended, do not override.

   .. py:attribute:: allow_reuse_address
      :annotation: = True

      


.. py:class:: _RequestHandler(request, client_address, server)

   Bases: :py:obj:`socketserver.BaseRequestHandler`

   Base class for request handler classes.

   This class is instantiated for each request to be handled.  The
   constructor sets the instance variables request, client_address
   and server, and then calls the handle() method.  To implement a
   specific service, all you need to do is to derive a class which
   defines a handle() method.

   The handle() method can find the request as self.request, the
   client address as self.client_address, and the server (in case it
   needs access to per-server information) as self.server.  Since a
   separate instance is created for each request, the handle() method
   can define other arbitrary instance variables.


   .. py:method:: handle(self)



.. py:function:: completions(text)


.. py:function:: add_history(line)


.. py:function:: history_item(index)


.. py:function:: is_complete(cmd)


.. py:function:: start_server(address)

   :param address: Server address
   :type address: tuple(str,int)


.. py:function:: send_sentinel(host, port)


