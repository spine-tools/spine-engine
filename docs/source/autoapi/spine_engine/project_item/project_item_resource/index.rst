:py:mod:`spine_engine.project_item.project_item_resource`
=========================================================

.. py:module:: spine_engine.project_item.project_item_resource

.. autoapi-nested-parse::

   Provides the ProjectItemResource class.

   :authors: M. Marin (KTH)
   :date:   29.4.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.project_item.project_item_resource.ProjectItemResource



Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.project_item.project_item_resource.database_resource
   spine_engine.project_item.project_item_resource.file_resource
   spine_engine.project_item.project_item_resource.transient_file_resource
   spine_engine.project_item.project_item_resource.file_resource_in_pack
   spine_engine.project_item.project_item_resource.extract_packs



.. py:class:: ProjectItemResource(provider_name, type_, label, url=None, metadata=None)

   Class to hold a resource made available by a project item and that may be consumed by another project item.

   .. attribute:: provider_name

      name of resource provider

      :type: str

   .. attribute:: type_

      resource's type

      :type: str

   .. attribute:: label

      an identifier string

      :type: str

   .. attribute:: metadata

      resource's metadata

      :type: dict

   :param provider_name: The name of the item that provides the resource
   :type provider_name: str
   :param type_: The resource type, currently available types:

                 - "file": url points to the file's path
                 - "file_pack": resource is part of a pack; url points to the file's path
                 - "database": url is the databases url
   :type type_: str
   :param label: A label that identifies the resource.
   :type label: str
   :param url: The url of the resource.
   :type url: str, optional
   :param metadata: Additional metadata providing extra information about the resource.
                    Currently available keys:

                    - filter_stack (str): resource's filter stack
                    - filter_id (str): filter id
   :type metadata: dict

   .. py:method:: clone(self, additional_metadata=None)

      Clones a resource and optionally updates the clone's metadata.

      :param additional_metadata: metadata to add to the clone
      :type additional_metadata: dict

      :returns: cloned resource
      :rtype: ProjectItemResource


   .. py:method:: __eq__(self, other)

      Return self==value.


   .. py:method:: __hash__(self)

      Return hash(self).


   .. py:method:: __repr__(self)

      Return repr(self).


   .. py:method:: url(self)
      :property:

      Resource URL.


   .. py:method:: path(self)
      :property:

      Returns the resource path in the local syntax, as obtained from parsing the url.


   .. py:method:: scheme(self)
      :property:

      Returns the resource scheme, as obtained from parsing the url.


   .. py:method:: hasfilepath(self)
      :property:


   .. py:method:: arg(self)
      :property:



.. py:function:: database_resource(provider_name, url, label=None)

   Constructs a database resource.

   :param provider_name: resource provider's name
   :type provider_name: str
   :param url: database URL
   :type url: str
   :param label: resource label
   :type label: str, optional


.. py:function:: file_resource(provider_name, file_path, label=None)

   Constructs a file resource.

   :param provider_name: resource provider's name
   :type provider_name: str
   :param file_path: path to file
   :type file_path: str
   :param label: resource label
   :type label: str, optional


.. py:function:: transient_file_resource(provider_name, label, file_path=None)

   Constructs a transient file resource.

   :param provider_name: resource provider's name
   :type provider_name: str
   :param label: resource label
   :type label: str
   :param file_path: file path if the file exists
   :type file_path: str, optional


.. py:function:: file_resource_in_pack(provider_name, label, file_path=None)

   Constructs a file resource that is part of a resource pack.

   :param provider_name: resource provider's name
   :type provider_name: str
   :param label: resource label
   :type label: str
   :param file_path: file path if the file exists
   :type file_path: str, optional


.. py:function:: extract_packs(resources)

   Extracts file packs from resources.

   :param resources: resources to process
   :type resources: Iterable of ProjectItemResource

   :returns: list of non-pack resources and dictionary of packs keyed by label
   :rtype: tuple


