######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Engine contributors
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
""" Split command line arguments. """


def split_cmdline_args(arg_string):
    """
    Splits a string of command line arguments into a list of tokens.

    Things in single ('') and double ("") quotes are kept as single tokens
    while the quotes themselves are stripped away.
    Thus, `--file="a long quoted 'file' name.txt` becomes ["--file=a long quoted 'file' name.txt"]

    Args:
        arg_string (str): command line arguments as a string

    Returns:
        list: a list of tokens
    """
    tokens = list()
    current_word = ""
    quoted_context = False
    for character in arg_string:
        if character in ("'", '"') and not quoted_context:
            quoted_context = character
        elif character == quoted_context:
            quoted_context = False
        elif not character.isspace() or quoted_context:
            current_word = current_word + character
        elif current_word:
            tokens.append(current_word)
            current_word = ""
    if current_word:
        tokens.append(current_word)
    return tokens
