:py:mod:`spine_engine.load_project_items`
=========================================

.. py:module:: spine_engine.load_project_items

.. autoapi-nested-parse::

   Functions to load project item modules.

   :author: A. Soininen (VTT)
   :date:   29.4.2020



Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.load_project_items.load_item_specification_factories
   spine_engine.load_project_items.load_executable_item_classes



.. py:function:: load_item_specification_factories(items_package_name)

   Loads the project item specification factories in given project item package.

   :param items_package_name: name of the package that contains the project items
   :type items_package_name: str

   :returns: a map from item type to specification factory
   :rtype: dict


.. py:function:: load_executable_item_classes(items_package_name)

   Loads the project item executable classes included in given project item package.

   :param items_package_name: name of the package that contains the project items
   :type items_package_name: str

   :returns: a map from item type to the executable item class
   :rtype: dict


