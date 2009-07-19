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
from __future__ import with_statement
from threading import Lock

import settings

from twisted.application import internet
from twisted.cred import checkers, portal
from twisted.spread import pb

from pydra_server.models import pydraSettings
from pydra_server.cluster.auth.master_realm import MasterRealm
from pydra_server.cluster.auth.rsa_auth import load_crypto
from pydra_server.cluster.auth.worker_avatar import WorkerAvatar
from pydra_server.cluster.constants import *
from pydra_server.cluster.module import Module


# init logging
import logging
logger = logging.getLogger('root')


class WorkerConnectionManager(Module):

    _signals = [
        'WORKER_CONNECTED',
        'WORKER_AUTHENTICATED',
        'WORKER_DISCONNECTED',
    ]

    _shared = [
        'worker_checker',
        '_workers'
    ]

    def __init__(self, manager):
        self._services = [self.get_worker_service]

        Module.__init__(self, manager)

        #locks
        self._lock = Lock() #general lock, use when multiple shared resources are touched

        #load rsa crypto
        self.pub_key, self.priv_key = load_crypto('./master.key')

        #cluster management
        self.workers = {}

        # setup worker security - using this checker just because we need
        # _something_ that returns an avatarID.  Its extremely vulnerable
        # but thats ok because the real authentication takes place after
        # the worker has connected
        self.worker_checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()


    def get_worker_service(self, master):
        """
        constructs a twisted service for Workers to connect to 
        """
        # setup cluster connections
        realm = MasterRealm()
        realm.server = self

        p = portal.Portal(realm, [self.worker_checker])
 
        return internet.TCPServer(pydraSettings.port, pb.PBServerFactory(p))


    def worker_authenticated(self, worker_avatar):
        """
        Callback when a worker has been successfully authenticated
        """
        self.emit('WORKER_CONNECTED', worker_avatar)
        #request status to determine what this worker was doing
        deferred = worker_avatar.remote.callRemote('status')
        deferred.addCallback(self.emit, 'WORKER_AUTHENTICATED', worker_avatar)


    def worker_disconnected(self, worker):
        """
        Callback from worker_avatar when it is disconnected
        """
        with self._lock:
            del self.workers[worker.name]

        self.emit('WORKER_DISCONNECTED', worker)
        

    

