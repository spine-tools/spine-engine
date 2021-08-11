:py:mod:`spine_engine.utils.serialization`
==========================================

.. py:module:: spine_engine.utils.serialization

.. autoapi-nested-parse::

   Functions to (de)serialize stuff.

   :authors: P. Savolainen (VTT)
   :date:   10.1.2018



Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.utils.serialization.path_in_dir
   spine_engine.utils.serialization.serialize_path
   spine_engine.utils.serialization.serialize_url
   spine_engine.utils.serialization.deserialize_path
   spine_engine.utils.serialization.deserialize_remote_path



.. py:function:: path_in_dir(path, directory)

   Returns True if the given path is in the given directory.


.. py:function:: serialize_path(path, project_dir)

   Returns a dict representation of the given path.

   If path is in project_dir, converts the path to relative.

   :param path: path to serialize
   :type path: str
   :param project_dir: path to the project directory
   :type project_dir: str

   :returns: Dictionary representing the given path
   :rtype: dict


.. py:function:: serialize_url(url, project_dir)

   Return a dict representation of the given URL.

   If the URL is a file that is in project dir, the URL is converted to a relative path.

   :param url: a URL to serialize
   :type url: str
   :param project_dir: path to the project directory
   :type project_dir: str

   :returns: Dictionary representing the URL
   :rtype: dict


.. py:function:: deserialize_path(serialized, project_dir)

   Returns a deserialized path or URL.

   :param serialized: a serialized path or URL
   :type serialized: dict
   :param project_dir: path to the project directory
   :type project_dir: str

   :returns: Path or URL as string
   :rtype: str


.. py:function:: deserialize_remote_path(serialized, base_path)


