######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Engine.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Module contains EventPublisher to which toolbox subscribers are registered as subscribers.

:author: R. Brady (UCD)
:date:   23.9.2020
"""


class EventPublisher:
    """
    An event based publisher that enables dag node started and dag node finished events to be
    dispatched to subscribers of this class.
    """

    def __init__(self, events=None):
        """
        Maps each named event to a dict
        Args:
            events (list(str)): list of named events e.g. ['exec_started', 'exec_finished']
        """
        if events is None:
            events = (
                'exec_started',
                'exec_finished',
                'log_event',
                'msg',
                'msg_success',
                'msg_warning',
                'msg_error',
                'msg_proc',
                'msg_proc_error',
                'msg_kernel_execution',
                'msg_standard_execution',
            )
        self.events = {event: dict() for event in events}

    def get_subscribers(self, event):
        """
        gets subscribers who are subscribed to event
        Args:
            event (str): named event to which subscribers are subscribed
        """
        return self.events[event]

    def register(self, event, subscriber, callback=None):
        """
        register a subscriber and its callback method to an event
        Args:
            event (str): named event to which subscribers are subscribed
            subscriber (Subscriber): a class designated as a subscriber, must have an update method
            callback (method): callback method of subscriber, is triggered when dispatch() is called
        """
        if callback is None:
            callback = getattr(subscriber, 'update')
        self.get_subscribers(event)[subscriber] = callback

    def unregister(self, event, subscriber):
        """
        removes a registered subscriber from an event
        Args:
            event (str): named event to which subscribers are subscribed
            subscriber (Subscriber): a class designated as a subscriber, must have an update method
        """
        del self.get_subscribers(event)[subscriber]

    def dispatch(self, event, node_data):
        """
        Iterate through subscribers of param: event and trigger subscriber callback method
        see _process_event() for use.
        Args:
             event (str): named event to which subscribers are subscribed
             node_data (dict()): dag node event data
        """
        for subscriber, callback in self.get_subscribers(event).items():
            callback(node_data)
