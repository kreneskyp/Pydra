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
from pydra_server.cluster.module import Module
from pydra_server.cluster.auth.worker_avatar import WorkerAvatar

# init logging
from pydra_server.logging.logger import init_logging
logger = init_logging(settings.LOG_FILENAME_MASTER)

class WorkerConnectionManager(Module):

    _signals = [
        'WORKER_CONNECTED',
        'WORKER_DISCONNECTED',
    ]

    def __init__(self, manager):
        #cluster management
        self.workers = {}


    def worker_authenticated(self, worker_avatar):
        """
        Callback when a worker has been successfully authenticated
        """
        #request status to determine what this worker was doing
        deferred = worker_avatar.remote.callRemote('status')
        deferred.addCallback(self.add_worker, worker=worker_avatar, worker_key=worker_avatar.name)


    def add_worker(self, result, worker, worker_key):
        """
        Add a worker avatar as worker available to the cluster.  There are two possible scenarios:
        1) Only the worker was started/restarted, it is idle
        2) Only master was restarted.  Workers previous status must be reestablished

        The best way to determine the state of the worker is to ask it.  It will return its status
        plus any relevent information for reestablishing it's status
        """
        # worker is working and it was the master for its task
        if result[0] == WORKER_STATUS_WORKING:
            logger.info('worker:%s - is still working' % worker_key)
            #record what the worker is working on
            #self._workers_working[worker_key] = task_key

        # worker is finished with a task
        elif result[0] == WORKER_STATUS_FINISHED:
            logger.info('worker:%s - was finished, requesting results' % worker_key)
            #record what the worker is working on
            #self._workers_working[worker_key] = task_key

            #check if the Worker acting as master for this task is ready
            if (True):
                #TODO
                pass

            #else not ready to send the results
            else:
                #TODO
                pass

        #otherwise its idle
        else:
            with self._lock:
                self.workers[worker_key] = worker
                # worker shouldn't already be in the idle queue but check anyway
                if not worker_key in self._workers_idle:
                    self._workers_idle.append(worker_key)
                    logger.info('worker:%s - added to idle workers' % worker_key)

