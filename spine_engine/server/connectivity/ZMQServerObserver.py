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
Contains ZMQServerObserver interface for receiving Zero MQ connections.
:authors: P. Pääkkönen (VTT)
:date:   20.08.2021
"""

from abc import abstractmethod
import ZMQConnection


class ZMQServerObserver:


    @abstractmethod
    def receiveConnection(self, conn:ZMQConnection) -> None:
        pass


