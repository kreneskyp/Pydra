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
from Crypto.PublicKey import RSA

import logging
logger = logging.getLogger('root')

class WorkerAvatar(RSAAvatar):
    """
    Avatar used by Workers connecting to the Master.
    """
    def __init__(self, name, server, node):
        self.name = name
        self.server = server

        node_key = node.load_pub_key()
        node_encrypt = node_key.encrypt if node_key else None
        master_encrypt = RSA.construct(server.pub_key).encrypt if server.pub_key else None

        RSAAvatar.__init__(self, master_encrypt, node_encrypt, server.worker_authenticated, True)

    def attached(self, mind):
        self.remote = mind

    def detached(self, mind):
        logger.info('worker:%s - disconnected' % self.name)
        if self.authenticated:
            self.server.remove_worker(self.name)
        self.remote = None


    def perspective_failed(self, message, workunit_key):
        """
        Called by workers when they task they were running threw an exception
        """
        return self.server.task_failed(self.name, message, workunit_key)


    def perspective_send_results(self, results, workunit_key):
        """
        Called by workers when they have completed their task and need to report the results.
        * Tasks runtime and log should be saved in the database
        """
        return self.server.send_results(self.name, results, workunit_key)

    def perspective_stopped(self):
        """
        Called by workers when they have stopped themselves because of a stop_task call
        This response may be delayed because there is no guaruntee that the Task will
        respect the STOP_FLAG.  Until this callback is made the Worker is still working
        """
        return self.server.worker_stopped(self.name)

    def perspective_request_worker(self, subtask_key, args, workunit_key):
        """
        Called by workers running a Parallel task.  This is a request
        for a worker in the cluster to process the args sent
        """
        return self.server.request_worker(self, subtask_key, args, workunit_key)


    def perspective_task_status(self):
        """
        returns the status (progress) of the task this worker is working on
        """
        return self.server.task_status(self)
