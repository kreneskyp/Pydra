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

from module_avatar import ModuleAvatar
from rsa_auth import RSAAvatar
from Crypto.PublicKey import RSA

import logging
logger = logging.getLogger('root')

class WorkerAvatar(ModuleAvatar, RSAAvatar):
    """
    Avatar used by Workers connecting to the Master.
    """
    def __init__(self, name, server, node):
        self.name = name
        self.server = server

        node_key = node.load_pub_key()
        node_key = node_key if node_key else None
        master_key = RSA.construct(server.pub_key) if server.pub_key else None

        ModuleAvatar.__init__(self, server.manager._remotes['REMOTE_WORKER'], master_key, None, node_key, server.worker_authenticated, True)
        RSAAvatar.__init__(self, master_key, None, node_key, server.worker_authenticated, True)


    def attached(self, mind):
        """
        callback when avatar is connected
        """
        self.remote = mind


    def detached(self, mind):
        """
        callback when avatar is disconnected
        """
        logger.info('worker:%s - disconnected' % self.name)
        if self.authenticated:
            self.server.worker_disconnected(self.name)
        self.remote = None
