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

from twisted.internet.defer import Deferred

from pydra_server.cluster.module import Module
from pydra_server.cluster.constants import *

import logging
logger = logging.getLogger('root')


class WorkerInfo():
    #worker info
    pid = None
    avatar = None
    worker_key = None

    # task info
    key = None
    args = None
    subtask_key = None
    workunit_key = None
    main_worker = None
    task_id = None


class WorkerManager(Module):

    _shared = ['master']

    def __init__(self, manager):
        self._remotes = [
            ('MASTER',self.init_node),

            # worker proxy - functions exposed to the master that are for the
            # most part just passed through to the worker
            ('MASTER', self.run_task),
            ('MASTER', self.stop_task),
            ('MASTER', self.worker_status),
            ('MASTER', self.task_status),
            ('MASTER', self.receive_results),

            # master proxy - functions exposed to the workers that are passed
            # through to the Master
            ('WORKER', self.send_results),
            ('WORKER', self.request_worker),
            ('WORKER', self.worker_stopped),
            ('WORKER', self.task_failed),
            ('WORKER', self.release_worker)

        ]

        self._listeners = {
            'WORKER_CONNECTED':self.run_task_delayed
        }

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
        with self.__lock:
            self.master_host = master_host
            self.master_port = master_port
            self.node_key = node_key

        self.emit('NODE_INITIALIZED', node_key)


    def proxy_to_master(self, remote, worker, *args, **kwargs):
        """
        Proxy a function to the master.  function and args are just
        passed through to the master
        """
        return self.master.remote.callRemote(remote, worker, *args, **kwargs)


    def proxy_to_worker(self, remote, worker_id, *args, **kwargs):
        """
        Proxy a function to a worker.  function and args are just
        passed through to the worker
        """
        worker = self.workers[worker_id]
        return worker.avatar.remote.callRemote(remote, *args, **kwargs)


    def receive_results(self, master, worker_key, *args, **kwargs):
        """
        Function called to make the subtask receive the results processed by another worker
        """
        return self.proxy_to_worker('receive_results', worker_id, *args, **kwargs)


    def release_worker(self, *args, **kwargs):
        return self.proxy_to_master('release_worker', *args, **kwargs)


    def request_worker(worker, *args, **kwargs):
        return self.proxy_to_master('request_worker', worker, *args, **kwargs)


    def run_task(self, avatar, worker_key, key, args={},
        subtask_key=None, workunit_key=None, main_worker=None, task_id=None):
        """
        Runs a task on this node.  The worker scheduler on master makes all
        decisions about which worker to run on.  This method only checks
        to see if the worker is already running or not.  This is a very
        explicit and deliberate division of repsonsibility.  Allowing
        decisions here dilutes the ability of the Master to control the
        cluster
        """
        logger.info('[Node] RunTask:  key=%s  args=%s  sub=%s  w=%s  main=%s' \
            % (key, args, subtask_key, workunit_key, \
            main_worker))
        worker = None

        with self.__lock:
            if worker_key in self.workers:
                # worker exists.  reuse it.
                logger.debug('RunTask - Using worker %s' % worker_key)
                worker = self.workers[worker_key]
                print '%%%%%%%%%%%%%', worker.avatar
                worker.deferred = worker.avatar.remote.callRemote('run_task', \
                        key, args, subtask_key, workunit_key, \
                        main_worker, task_id)
            else:
                # worker not running.  start it saving the information required
                # to start the subtask.  This function will return a deferred
                # to the master.  The deferred will be
                logger.debug('RunTask - Spawning worker: %s', key)
                worker = WorkerInfo()
                worker.worker_key = worker_key
                worker.deferred = Deferred()
                worker.pid = Popen(["python", "pydra_server/cluster/worker/worker.py", worker_key, '18800']).pid
                self.workers[worker_key] = worker

            worker.key = key
            worker.args = args
            worker.subtask_key = subtask_key
            worker.workunit_key = workunit_key
            worker.main_worker = main_worker
            worker.task_id=task_id

            return worker.deferred


    def run_task_delayed(self, worker_avatar):
        """
        Callback when a worker has started.  start the intended task.  Attach
        the deferred originally returned in run_task to the deferred returned
        from worker.run_task.  This will cause the result to propagate through
        the deferreds back to master.
        """
        w = self.workers[worker_avatar.name]
        w.avatar = worker_avatar
        deferred = self.run_task(None, w.worker_key, w.key, w.args, w.subtask_key, \
                    w.workunit_key, w.main_worker, w.task_id)
        deferred.addCallback(w.deferred.callback)


    def send_results(self, worker, *args, **kwargs):
        return self.proxy_to_master('send_results', worker, *args, **kwargs)


    def stop_task(self, master, worker_id):
        return self.proxy_to_worker('stop_task', worker_id)


    def task_failed(self, *args, **kwargs):
        return self.proxy_to_master('task_failed', *args, **kwargs)


    def task_status(self, master, worker_id):
        """
        Returns status of task this task is performing
        """
        if worker_id in self.workers and self.workers[worker_id].avatar:
            return self.proxy_to_worker('task_status', worker_id)
        else:
            return -1


    def worker_status(self, master, worker_id):
        """
        Return the status of the current task if running, else None
        """
        with self.__lock:
            if worker_id in self.workers:
                return self.proxy_to_worker('status', worker_id)

            else:
                return (WORKER_STATUS_IDLE,)


    def worker_stopped(self, *args, **kwargs):
        return self.proxy_to_master('worker_stopped', *args, **kwargs)


