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

from twisted.application import internet
from twisted.cred import portal, checkers
from twisted.spread import pb
from zope.interface import implements

import pydra_settings
from pydra.cluster.auth.rsa_auth import load_crypto
from pydra.cluster.auth.master_avatar import MasterAvatar
from pydra.cluster.module import Module

import logging
logger = logging.getLogger('root')


class ClusterRealm:
    implements(portal.IRealm)
    def requestAvatar(self, avatarID, mind, *interfaces):
        assert pb.IPerspective in interfaces
        avatar = MasterAvatar(avatarID, self.server)
        avatar.attached(mind)
        return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)


class MasterConnectionManager(Module):

    
    _shared = [
        'port',
        'host',
        'master'
    ]

    def __init__(self):

        self._services = [
            self.get_service
        ]


    def _register(self, manager):
        Module._register(self, manager)        
        self.port = pydra_settings.PORT
        self.node_key = None
        #load crypto keys for authentication
        self.pub_key, self.priv_key = load_crypto('%s/node.key' % \
                pydra_settings.RUNTIME_FILES_DIR)
        self.master_pub_key = load_crypto('%s/node.master.key' % \
                pydra_settings.RUNTIME_FILES_DIR, create=False, both=False)
        self.master = None


    def get_service(self, manager):
        """
        Creates a service object that can be used by twistd init code to start the server
        """
        logger.info('MasterConnectionManager - starting server on port %s' % self.port)

        realm = ClusterRealm()
        realm.server = self

        # create security - Twisted does not support ssh in the pb so were doing our
        # own authentication until it is implmented, leaving in the memory
        # checker just so we dont have to rip out the authentication code
        from twisted.cred.checkers import InMemoryUsernamePasswordDatabaseDontUse
        checker =   InMemoryUsernamePasswordDatabaseDontUse()
        checker.addUser('master','1234')
        p = portal.Portal(realm, [checker])

        factory = pb.PBServerFactory(p)

        return internet.TCPServer(self.port, factory)


    def master_authenticated(self, master_avatar):
        """
        callback made by MasterAvatar when it successfully connects
        """
        self.master = master_avatar
