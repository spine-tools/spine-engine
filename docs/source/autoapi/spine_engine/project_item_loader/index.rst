:py:mod:`spine_engine.project_item_loader`
==========================================

.. py:module:: spine_engine.project_item_loader

.. autoapi-nested-parse::

   Contains :class:`ProjectItemLoader`.

   :author: A. Soininen (VTT)
   :date:   11.2.2021



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.project_item_loader.ProjectItemLoader




.. py:class:: ProjectItemLoader

   A singleton class for loading project items from multiple processes simultaneously.

   .. py:attribute:: _specification_factories
      

      

   .. py:attribute:: _executable_item_classes
      

      

   .. py:attribute:: _specification_factories_lock
      

      

   .. py:attribute:: _executable_item_classes_lock
      

      

   .. py:method:: load_item_specification_factories(self, items_module_name)

      Loads the project item specification factories in the standard Toolbox package.

      :param items_module_name: name of the Python module that contains the project items
      :type items_module_name: str

      :returns: a map from item type to specification factory
      :rtype: dict


   .. py:method:: load_executable_item_classes(self, items_module_name)

      Loads the project item executable classes included in the standard Toolbox package.

      :param items_module_name: name of the Python module that contains the project items
      :type items_module_name: str

      :returns: a map from item type to the executable item class
      :rtype: dict



