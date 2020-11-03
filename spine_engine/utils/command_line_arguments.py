######################################################################################################################
# Copyright (C) 2017-2020 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Split command line arguments, expand tags.

:authors: P. Savolainen (VTT)
:date:   10.1.2018
"""

import re
from collections import ChainMap


CMDLINE_TAG_EDGE = "@@"


class CmdlineTag:
    URL = CMDLINE_TAG_EDGE + "url:<data-store-name>" + CMDLINE_TAG_EDGE
    URL_INPUTS = CMDLINE_TAG_EDGE + "url_inputs" + CMDLINE_TAG_EDGE
    URL_OUTPUTS = CMDLINE_TAG_EDGE + "url_outputs" + CMDLINE_TAG_EDGE
    OPTIONAL_INPUTS = CMDLINE_TAG_EDGE + "optional_inputs" + CMDLINE_TAG_EDGE


def expand_tags(args, optional_input_files, input_urls, output_urls):
    """"
    Expands first @@ tags found in given list of command line arguments.

    Args:
        args (list): a list of command line arguments
        optional_input_files (list): a list of Tool's optional input file names
        input_urls (dict): a mapping from URL provider (input Data Store name) to URL string
        output_urls (dict): a mapping from URL provider (output Data Store name) to URL string

    Returns:
        tuple: a boolean flag, if True, indicates that tags were expanded and a list of
            expanded command line arguments
    """

    def expand_list(arg, tag, things, expanded_args):
        preface, tag_found, postscript = arg.partition(tag)
        if tag_found:
            if things:
                first_input_arg = preface + things[0]
                expanded_args.append(first_input_arg)
                expanded_args += things[1:]
                expanded_args[-1] = expanded_args[-1] + postscript
            else:
                expanded_args.append(preface + postscript)
            return True
        return False

    expanded_args = list()
    named_data_store_tag_fingerprint = re.compile(CMDLINE_TAG_EDGE + "url:.+" + CMDLINE_TAG_EDGE)
    all_urls = ChainMap(input_urls, output_urls)
    input_url_list = list(input_urls.values())
    output_url_list = list(output_urls.values())
    did_expand = False
    for arg in args:
        if expand_list(arg, CmdlineTag.OPTIONAL_INPUTS, optional_input_files, expanded_args):
            did_expand = True
            continue
        if expand_list(arg, CmdlineTag.URL_INPUTS, input_url_list, expanded_args):
            did_expand = True
            continue
        if expand_list(arg, CmdlineTag.URL_OUTPUTS, output_url_list, expanded_args):
            did_expand = True
            continue
        match = named_data_store_tag_fingerprint.search(arg)
        if match:
            preface = arg[: match.start()]
            tag = match.group()
            postscript = arg[match.end() :]
            data_store_name = tag[6:-2]
            try:
                url = all_urls[data_store_name]
            except KeyError:
                raise RuntimeError(f"Cannot replace tag '{tag}' since '{data_store_name}' was not found.")
            expanded_args.append(preface + url + postscript)
            did_expand = True
            continue
        expanded_args.append(arg)
    return did_expand, expanded_args


def split_cmdline_args(arg_string):
    """
    Splits a string of command line into a list of tokens.

    Things in single ('') and double ("") quotes are kept as single tokens
    while the quotes themselves are stripped away.
    Thus, `--file="a long quoted 'file' name.txt` becomes ["--file=a long quoted 'file' name.txt"]

    Args:
        arg_string (str): command line arguments as a string

    Returns:
        list: a list of tokens
    """
    # The expandable tags may include whitespaces, particularly in Data Store names.
    # We replace the tags temporarily by '@_@_@' to simplify splitting
    # and put them back to the args list after the string has been split.
    tag_safe = list()
    tag_fingerprint = re.compile(CMDLINE_TAG_EDGE + "url:.+?" + CMDLINE_TAG_EDGE)
    match = tag_fingerprint.search(arg_string)
    while match:
        tag_safe.append(match.group())
        arg_string = arg_string[: match.start()] + "@_@_@" + arg_string[match.end() :]
        match = tag_fingerprint.search(arg_string)
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
        else:
            tokens.append(current_word)
            current_word = ""
    if current_word:
        tokens.append(current_word)
    for index, token in enumerate(tokens):
        preface, tag_token, prologue = token.partition("@_@_@")
        if tag_token:
            tokens[index] = preface + tag_safe.pop(0) + prologue
    return tokens
