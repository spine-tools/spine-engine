:py:mod:`spine_engine.utils.queue_logger`
=========================================

.. py:module:: spine_engine.utils.queue_logger

.. autoapi-nested-parse::

   The QueueLogger class.

   :authors: M. Marin (KTH)
   :date:   3.11.2020



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   spine_engine.utils.queue_logger._Message
   spine_engine.utils.queue_logger._Prompt
   spine_engine.utils.queue_logger._ExecutionMessage
   spine_engine.utils.queue_logger.QueueLogger




.. py:class:: _Message(queue, event_type, msg_type, item_name)

   .. py:method:: filter_id(self)
      :property:


   .. py:method:: emit(self, msg_text)



.. py:class:: _Prompt(queue, item_name, prompt_queue)

   .. py:method:: filter_id(self)
      :property:


   .. py:method:: emit(self, prompt)



.. py:class:: _ExecutionMessage(queue, event_type, item_name)

   .. py:method:: filter_id(self)
      :property:


   .. py:method:: emit(self, msg)



.. py:class:: QueueLogger(queue, item_name, prompt_queue)

   A :class:`LoggerInterface` compliant logger that puts messages into a Queue.


   .. py:method:: set_filter_id(self, filter_id)



