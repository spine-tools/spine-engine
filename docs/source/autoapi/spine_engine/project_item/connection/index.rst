:py:mod:`spine_engine.project_item.connection`
==============================================

.. py:module:: spine_engine.project_item.connection

.. autoapi-nested-parse::

   Provides the :class:`Connection` class.

   :authors: A. Soininen (VTT)
   :date:    12.2.2021



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.project_item.connection.Connection




.. py:class:: Connection(source_name, source_position, destination_name, destination_position, resource_filters=None, options=None)

   Represents a connection between two project items.

   :param source_name: source project item's name
   :type source_name: str
   :param source_position: source anchor's position
   :type source_position: str
   :param destination_name: destination project item's name
   :type destination_name: str
   :param destination_position: destination anchor's position
   :type destination_position: str
   :param resource_filters: mapping from resource labels and filter types to
                            database ids and activity flags
   :type resource_filters: dict, optional
   :param options: any options, at the moment only has "use_datapackage"
   :type options: dict, optional

   .. py:method:: __eq__(self, other)

      Return self==value.


   .. py:method:: name(self)
      :property:


   .. py:method:: destination_position(self)
      :property:

      Anchor's position on destination item.


   .. py:method:: source_position(self)
      :property:

      Anchor's position on source item.


   .. py:method:: database_resources(self)
      :property:

      Connection's database resources


   .. py:method:: has_filters(self)

      Return True if connection has filters.

      :returns: True if connection has filters, False otherwise
      :rtype: bool


   .. py:method:: resource_filters(self)
      :property:

      Connection's resource filters.


   .. py:method:: use_datapackage(self)
      :property:


   .. py:method:: id_to_name(self, id_, filter_type)

      Map from scenario/tool database id to name


   .. py:method:: receive_resources_from_source(self, resources)

      Receives resources from source item.

      :param resources: source item's resources
      :type resources: Iterable of ProjectItemResource


   .. py:method:: replace_resource_from_source(self, old, new)

      Replaces an existing resource.

      :param old: old resource
      :type old: ProjectItemResource
      :param new: new resource
      :type new: ProjectItemResource


   .. py:method:: fetch_database_items(self)

      Reads filter information from database.


   .. py:method:: set_online(self, resource, filter_type, online)

      Sets the given filters online or offline.

      :param resource: Resource label
      :type resource: str
      :param filter_type: Either SCENARIO_FILTER_TYPE or TOOL_FILTER_TYPE, for now.
      :type filter_type: str
      :param online: mapping from scenario/tool id to online flag
      :type online: dict


   .. py:method:: convert_resources(self, resources)

      Called when advertising resources through this connection *in the FORWARD direction*.
      Takes the initial list of resources advertised by the source item and returns a new list,
      which is the one finally advertised.

      At the moment it only packs CSVs into datapackage (and again, it's only used in the FORWARD direction).

      :param resources: Resources to convert
      :type resources: list of ProjectItemResource

      :returns: list of ProjectItemResource


   .. py:method:: to_dict(self)

      Returns a dictionary representation of this Connection.

      :returns: serialized Connection
      :rtype: dict


   .. py:method:: from_dict(connection_dict)
      :staticmethod:

      Restores a connection from dictionary.

      :param connection_dict: connection dictionary
      :type connection_dict: dict

      :returns: restored connection
      :rtype: Connection



