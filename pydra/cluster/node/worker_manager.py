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

from subprocess import Popen
from threading import Lock


from pydra.cluster.module import Module

import logging
logger = logging.getLogger('root')


class WorkerManager(Module):

    _shared = ['info']    

    def __init__(self, manager):

        self._remotes = [
            ('MASTER',self.init_node)
        ]

        Module.__init__(self, manager)

        self.__lock = Lock()
        self.workers = {}
        self.initialized = False


    def init_node(self, avatar_name, master_host, master_port, node_key):
        """
        Initializes the node so it ready for use.  Workers will not be started
        until the master makes this call.  After a node is initialized workers
        should be able to reconnect if a connection is lost
        """

        # only initialize the node if it has not been initialized yet.
        # its possible for the server to be restarted without affecting
        # the state of the nodes
        if not self.initialized:
            with self.__lock:
                self.master_host = master_host
                self.master_port = master_port
                self.node_key = node_key

            #start the workers
            self.start_workers()
            self.initialized = True


    def start_workers(self):
        """
        Starts all of the workers.  By default there will be one worker for
        each core
        """

        # dirty hack to locate worker file.  This can later be replaced/
        import pydra
        pydra_root = pydra.__file__[:pydra.__file__.rfind('/')]
        print '%s/cluster/worker/worker.py' % pydra_root
        self.pids = [
            Popen([
                    'python',
                    '%s/cluster/worker/worker.py' % pydra_root,
                    self.master_host,
                    str(self.master_port),
                    self.node_key, '%s:%s' % (self.node_key, i)
                ]).pid 
            for i in range(self.info['cores'])
            ]
