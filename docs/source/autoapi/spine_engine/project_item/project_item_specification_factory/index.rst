:py:mod:`spine_engine.project_item.project_item_specification_factory`
======================================================================

.. py:module:: spine_engine.project_item.project_item_specification_factory

.. autoapi-nested-parse::

   Contains project item specification factory.

   :authors: A. Soininen (VTT)
   :date:   6.5.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.project_item.project_item_specification_factory.ProjectItemSpecificationFactory




.. py:class:: ProjectItemSpecificationFactory

   A factory to make project item specifications.

   .. py:method:: item_type()
      :staticmethod:
      :abstractmethod:

      Returns the project item's type.


   .. py:method:: make_specification(definition, app_settings, logger)
      :staticmethod:
      :abstractmethod:

      Makes a project item specification.

      :param definition: specification's definition dictionary
      :type definition: dict
      :param app_settings: Toolbox settings
      :type app_settings: QSettings
      :param logger: a logger
      :type logger: LoggerInterface

      :returns: a specification built from the given definition
      :rtype: ProjectItemSpecification



