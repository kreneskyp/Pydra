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
from threading import RLock

from twisted.internet.defer import Deferred

from pydra_server.cluster.auth.worker_avatar import WorkerAvatar
from pydra_server.cluster.module import Module
from pydra_server.cluster.constants import *

import logging
logger = logging.getLogger('root')


class WorkerManager(Module):

    _shared = ['master', 'workers', 'worker_connection_manager']

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
            ('MASTER', self.receive_results),
            ('MASTER', self.release_worker),

            # master proxy - functions exposed to the workers that are passed
            # through to the Master
            ('WORKER', self.send_results),
            ('WORKER', self.request_worker),
            ('WORKER', self.worker_stopped),
            ('WORKER', self.task_failed),
            ('WORKER', self.request_worker_release)

        ]

        self._listeners = {
            'WORKER_CONNECTED':self.run_task_delayed,
            'WORKER_DISCONNECTED':self.clean_up_finished_worker
        }

        Module.__init__(self, manager)

        self.__lock = RLock()
        self.workers = {}
        self.workers_finishing = []
        self.initialized = False


    def clean_up_finished_worker(self, worker):
        """
        Called when workers disconnect.  When a worker shuts itself down it
        becomes a zombie process (defunct) and while it does not use resources
        it does exist in the process table.  Just to keep things nice and tidy
        this function will kill any worker that disconnected and has the
        finished flag set
        """
        if worker.finished:
            worker.popen.wait()


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


    def kill_worker(self, worker_key, kill=False):
        """
        Stops a worker process.

        @param worker_key: key of worker to kill
        @param kill: send SIGKILL instead of SIGTERM. Default is signal
                     is SIGTERM.  SIGKILL should only be used in extreme cases
        """
        with self.__lock:
            worker = self.workers[worker_key]
            logger.debug('Stopping %s with %s' % \
                        (worker_key, 'SIGKILL' if kill else 'SIGTERM'))

            # python >= 2.6 required for Popen.kill()
            if 'kill' in Popen.__dict__:
                if kill:
                    worker.popen.kill()
                else:
                    worker.popen.terminate()

            else:
                import os
                if kill:
                    os.system('kill -9 %s' % worker.popen.pid)
                else:
                    os.system('kill %s' % worker.popen.pid)

            if worker.name in self.workers:
                worker.finished = True
                del self.workers[worker_key]


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
        return worker.remote.callRemote(remote, *args, **kwargs)


    def receive_results(self, master, *args, **kwargs):
        """
        Function called to make the subtask receive the results processed by
        another worker
        """
        return self.proxy_to_worker('receive_results', *args, **kwargs)


    def release_worker(self, master, *args, **kwargs):
        return self.proxy_to_worker('release_worker', *args, **kwargs)


    def request_worker(self, *args, **kwargs):
        return self.proxy_to_master('request_worker', *args, **kwargs)


    def request_worker_release(self, *args, **kwargs):
        return self.proxy_to_master('request_worker_release', *args, **kwargs)


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
                worker.run_task_deferred = worker.remote.callRemote('run_task',\
                        key, args, subtask_key, workunit_key, \
                        main_worker, task_id)
            else:
                # worker not running. start it saving the information required
                # to start the subtask.  This function will return a deferred
                # to the master.  The deferred will be
                logger.debug('RunTask - Spawning worker: %s', key)
                worker = WorkerAvatar(self.worker_connection_manager, \
                                                            worker_key)
                worker.worker_key = worker_key
                worker.run_task_deferred = Deferred()
                worker.popen = Popen(["python", \
                                    "pydra_server/cluster/worker/worker.py", \
                                    worker_key, '18800'])
                self.workers[worker_key] = worker

            worker.key = key
            worker.args = args
            worker.workunit_key = workunit_key
            worker.main_worker = main_worker
            worker.task_id=task_id
            if worker_key == main_worker:
                worker.local_subtask = subtask_key
            else:
                worker.subtask_key = subtask_key

            return worker.run_task_deferred


    def run_task_delayed(self, worker):
        """
        Callback when a worker has started.  start the intended task.  Attach
        the deferred originally returned in run_task to the deferred returned
        from worker.run_task.  This will cause the result to propagate through
        the deferreds back to master.
        """ 
        sent_deferred = worker.run_task_deferred
        deferred = self.run_task(None, worker.worker_key, worker.key, \
                    worker.args, worker.subtask_key, worker.workunit_key, \
                    worker.main_worker, worker.task_id)
        deferred.addCallback(sent_deferred.callback)


    def send_results(self, worker_key, results, *args, **kwargs):
        """
        Proxy for sending results to master.  Once this method is called the
        Node takes responsibility for ensuring that the results are delivered.
        This includes failover for when the Master goes away.

        If this is the mainworker for a task the worker will shut itself down
        as soon as it receives confirmation that the results have been handed 
        off to this Node.
        """
        with self.__lock:
            worker = self.workers[worker_key]

            if worker.main_worker == worker_key and not worker.local_subtask:
                del self.workers[worker_key]
                worker.finished = True
            else:
                # worker may be reused to clear all args to avoid confusion
                worker.subtask_key = None
                worker.local_subtask = None

            deferred = self.proxy_to_master('send_results', worker_key, \
                                                results, *args, **kwargs)
            worker.results = results
            # deferred.addErrback(self.send_results_failed, worker)

        return worker.finished


    def send_results_failed(self, worker):
        """
        Errback called when sending results to the master fails.  resend when
        master reconnects
        """
        with self._lock_connection:
            # check again for master. The lock is released so its possible
            # master could connect and be set before we set the flags 
            # indicating the problem
            if self.master:
                # reconnected, just resend the call.  The call is recursive
                # from this point if by some odd chance it disconnects again 
                # while sending
                logger.error('results failed to send by master is still here')
                #deferred = self.master.callRemote("send_results", task_results, task_results)
                #deferred.addErrback(self.send_results_failed, task_results, task_results)

            else:
                # nope really isn't connected.  set flag.  even if connection 
                # is in progress this thread has the lock and reconnection cant 
                # finish till we release it
                self.workers_finishing.append(worker)


    def stop_task(self, master, worker_id):
        return self.proxy_to_worker('stop_task', worker_id)


    def task_failed(self, *args, **kwargs):
        return self.proxy_to_master('task_failed', *args, **kwargs)


    def task_status(self, master, worker_id):
        """
        Returns status of task this task is performing
        """
        if worker_id in self.workers and self.workers[worker_id].remote:
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


