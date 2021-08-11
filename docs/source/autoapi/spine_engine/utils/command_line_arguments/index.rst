:py:mod:`spine_engine.utils.command_line_arguments`
===================================================

.. py:module:: spine_engine.utils.command_line_arguments

.. autoapi-nested-parse::

   Split command line arguments.

   :authors: P. Savolainen (VTT)
   :date:   10.1.2018



Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   spine_engine.utils.command_line_arguments.split_cmdline_args



.. py:function:: split_cmdline_args(arg_string)

   Splits a string of command line arguments into a list of tokens.

   Things in single ('') and double ("") quotes are kept as single tokens
   while the quotes themselves are stripped away.
   Thus, `--file="a long quoted 'file' name.txt` becomes ["--file=a long quoted 'file' name.txt"]

   :param arg_string: command line arguments as a string
   :type arg_string: str

   :returns: a list of tokens
   :rtype: list


