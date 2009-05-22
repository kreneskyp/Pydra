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

from rsa_auth import RSAAvatar

import logging
logger = logging.getLogger('root')

class MasterAvatar(RSAAvatar):
    """
    Avatar that exposes and controls what a Master can do on this Node
    """
    def __init__(self, name, server):
        self.name = name
        self.server = server

        node_encrypt = server.priv_key.encrypt if server.priv_key else None
        master_encrypt = server.master_pub_key.encrypt if server.master_pub_key else None

        RSAAvatar.__init__(self, node_encrypt, master_encrypt, no_key_first_use=True)
        logger.info('Master connected to node')


    # returns the status of this node
    def perspective_status(self):
        if self.authenticated:
            pass


    # Returns a dictionary of useful information about this node
    def perspective_info(self):
        if self.authenticated:
            return self.server.info



    def perspective_init(self, master_host, master_port, node_key, master_pub_key=None):
        """
        Initializes a node.  The server sends its connection information and
        credentials for the node
        """
        if self.authenticated:
            return self.server.init_node(master_host, master_port, node_key, master_pub_key)