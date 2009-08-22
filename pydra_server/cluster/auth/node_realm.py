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
import settings
from zope.interface import implements

from twisted.cred import portal
from twisted.spread import pb

from pydra_server.cluster.auth.worker_avatar import WorkerAvatar

# init logging
import logging
logger = logging.getLogger('root')

class NodeRealm:
    """
    Realm used by the Master server to assign avatars.
    """
    implements(portal.IRealm)
    def requestAvatar(self, avatarID, mind, *interfaces):
        assert pb.IPerspective in interfaces

        avatar = WorkerAvatar(self.server, avatarID)
        avatar.attached(mind)
        logger.info('worker:%s - connected' % avatarID)

        return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)
