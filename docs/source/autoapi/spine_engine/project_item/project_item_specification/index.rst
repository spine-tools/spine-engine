:py:mod:`spine_engine.project_item.project_item_specification`
==============================================================

.. py:module:: spine_engine.project_item.project_item_specification

.. autoapi-nested-parse::

   Contains project item specification class.

   :authors: M. Marin (KTH)
   :date:    7.5.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.project_item.project_item_specification.ProjectItemSpecification




.. py:class:: ProjectItemSpecification(name, description=None, item_type='', item_category='')

   Class to hold a project item specification.

   .. attribute:: item_type

      type of the project item the specification is compatible with

      :type: str

   .. attribute:: definition_file_path

      specification's JSON file path

      :type: str

   :param name: specification name
   :type name: str
   :param description: description
   :type description: str
   :param item_type: Project item type
   :type item_type: str
   :param item_category: Project item category
   :type item_category: str

   .. py:method:: set_name(self, name)

      Set object name and short name.
      Note: Check conflicts (e.g. name already exists)
      before calling this method.

      :param name: New (long) name for this object
      :type name: str


   .. py:method:: set_description(self, description)

      Set object description.

      :param description: Object description
      :type description: str


   .. py:method:: save(self)
      :abstractmethod:

      Writes the specification to the path given by ``self.definition_file_path``

      :returns: True if the operation was successful, False otherwise
      :rtype: bool


   .. py:method:: to_dict(self)
      :abstractmethod:

      Returns a dict for the specification.

      :returns: specification dict
      :rtype: dict


   .. py:method:: is_equivalent(self, other)
      :abstractmethod:

      Returns True if two specifications are essentially the same.

      :param other: specification to compare to
      :type other: DataTransformerSpecification

      :returns: True if the specifications are equivalent, False otherwise
      :rtype: bool



