"""
    Copyright 2009 Oregon State University

    This file is part of Pydra.

    Pydra is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Pydra is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Pydra.  If not, see <http://www.gnu.org/licenses/>.
"""

import platform
from pydra.cluster.module import Module
from pydra.util.zero_conf_service import ZeroConfService

class NodeZeroConfService(Module):
    """
    Module that publishes the node port using ZeroConfService (avahi)
    """

    _shared = [
        'port',
        'host'
    ]

    def __init__(self):

        self._listeners = {
            'MANAGER_INIT':self.start_service
        }


    def start_service(self):
        """
        Starts the zeroconfservice
        """
        self.service = ZeroConfService(name=platform.node(), port=self.port,
            stype="_pydra._tcp")
        self.service.publish()
