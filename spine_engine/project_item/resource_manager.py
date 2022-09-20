######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Provides the ResourceManager class.

:authors: M. Marin (ER)
:date:   20.9.2022
"""
import threading
from contextlib import contextmanager


class ResourceManager:
    def __init__(self):
        self._conditions = {}
        self._checkouts = {}

    def _get_condition(self, resource_id):
        if resource_id not in self._conditions:
            self._conditions[resource_id] = threading.Condition()
        return self._conditions[resource_id]

    def _wait_for_precursors(self, resource_id, precursors):
        if not precursors:
            return
        condition = self._get_condition(resource_id)
        with condition:
            while not all(p in self._checkouts.get(resource_id, []) for p in precursors):
                condition.wait()

    def _check_out_consumer(self, resource_id, consumer):
        condition = self._get_condition(resource_id)
        with condition:
            self._checkouts.setdefault(resource_id, set()).add(consumer)
            condition.notify_all()

    @contextmanager
    def managing_order(self, resource):
        """A context manager that ensures that a shared resource is used in the right order by a number of concurrent
        consumers.

        Args:
            resource (ProjectItemResource): the resource. If metadata has a 'precursors' key, then wait until all
            the items in that list have checked out on the resource before entering the context.
            If metadata has a 'consumer' key, then check out that item when exiting the context.
        """
        self._wait_for_precursors(resource.identifier, resource.metadata.get("precursors"))
        try:
            yield None
        finally:
            self._check_out_consumer(resource.identifier, resource.metadata.get("consumer"))
