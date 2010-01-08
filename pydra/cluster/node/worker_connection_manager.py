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

from twisted.application import internet
from twisted.cred import checkers, portal
from twisted.spread import pb

import pydra_settings
from pydra.cluster.auth.node_realm import NodeRealm
from pydra.cluster.auth.rsa_auth import load_crypto
from pydra.cluster.auth.worker_avatar import WorkerAvatar
from pydra.cluster.constants import *
from pydra.cluster.module import Module


# init logging
import logging
logger = logging.getLogger('root')


class WorkerConnectionManager(Module):

    _signals = [
        'WORKER_CONNECTED',
        'WORKER_DISCONNECTED',
    ]

    _shared = [
        'workers',
        'info',
        'worker_connection_manager'
    ]

    def __init__(self):
        self._services = [self.get_worker_service]
        self._listeners = {
                    'NODE_INITIALIZED':self.enable_workers,
                    'WORKER_FINISHED':self.remove_worker
                }

        #locks
        self._lock = Lock() #general lock, use when multiple shared resources are touched

        #load rsa crypto
        self.pub_key, self.priv_key = load_crypto('%s/node.key' % \
                pydra_settings.RUNTIME_FILES_DIR)

        # setup worker security - using this checker just because we need
        # _something_ that returns an avatarID.  Its extremely vulnerable
        # but thats ok because the real authentication takes place after
        # the worker has connected
        self.worker_checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()

    def _register(self, manager):
        Module._register(self, manager)
        self.worker_connection_manager = self
        self.workers = {}


    def enable_workers(self, node_key):
        """
        Enables workers to login.  cannot happen until the Node has received
        its node_key from the master.
        """
        for i in range(self.info['cores']):
            logger.debug('enabling worker: %s:%i' % (node_key, i) )
            self.worker_checker.addUser('%s:%s' % (node_key, i) , '1234')


    def get_worker_service(self, master):
        """
        constructs a twisted service for Workers to connect to 
        """
        logger.info('WorkerConnectionManager - starting server on port %s' % pydra_settings.WORKER_PORT)

        # setup cluster connections
        realm = NodeRealm()
        realm.server = self

        p = portal.Portal(realm, [self.worker_checker])
 
        return internet.TCPServer(pydra_settings.WORKER_PORT, pb.PBServerFactory(p))


    def remove_worker(self, worker):
        """
        Removes a worker from the pool of workers.  This is usually done when
        the worker is shutting down but has not disconnected yet.
        
        @param worker - worker avatar
        """
        with self._lock:
            if worker.name in self.workers:
                logger.debug('Removing worker from pool: %s' % worker.name)
                del self.workers[worker.name]


    def worker_authenticated(self, worker):
        """
        Callback when a worker has been successfully authenticated
        
        @param worker - worker avatar
        """
        with self._lock:
            self.workers[worker.name] = worker
        self.emit('WORKER_CONNECTED', worker)


    def worker_disconnected(self, worker):
        """
        Callback from worker_avatar when it is disconnected
        
        @param worker - worker avatar
        """
        with self._lock:
            if not worker.finished and worker.name in self.workers:
                logger.debug('Removing worker from pool: %s' % worker.name)
                del self.workers[worker.name]
        self.emit('WORKER_DISCONNECTED', worker)

    

