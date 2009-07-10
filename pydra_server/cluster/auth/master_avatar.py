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

        node_key = server.priv_key if server.priv_key else None
        node_pub_key = server.pub_key if server.pub_key else None
        self.master_key = server.master_pub_key if server.master_pub_key else None

        RSAAvatar.__init__(self, node_key, node_pub_key, self.master_key, save_key=self.save_key)
        logger.info('Master connected to node')


    def perspective_get_key(self):
        """
        Return the node's public key, so we can do a duplicate detection on the
        master side
        """
        return self.chunks()

    def save_key(self, json_key):
        """
        Callback to save public key from the master        
        
        only save the key if its new.  It is safe to update the key if 
        the master is authenticated but that process is complicated.
        it requires logic to ensure that all Nodes receive the new key
        for now we're avoiding that.
        """
        if not self.master_key:
            import os
            import simplejson
            from Crypto.PublicKey import RSA
            from twisted.conch.ssh.keys import Key

            key = simplejson.loads(json_key)
            key = [long(x) for x in key]
            rsa_key = RSA.construct(key)
            self.master_key = rsa_key
            self.server.master_pub_key = rsa_key

            key_file = None
            try:            
                key_file = file('./node.master.key', 'w')
                logger.info('saving new master key')
                key_file = key_file.write(json_key)
                os.chmod('./node.master.key', 0400)                
            finally:
                if key_file:
                    key_file.close()


    # returns the status of this node
    def perspective_status(self):
        if self.authenticated:
            pass


    # Returns a dictionary of useful information about this node
    def perspective_info(self):
        if self.authenticated:
            return self.server.info


    def perspective_init(self, master_host, master_port, node_key):
        """
        Initializes a node.  The server sends its connection information and
        credentials for the node
        """
        if self.authenticated:
            return self.server.init_node(master_host, master_port, node_key)
