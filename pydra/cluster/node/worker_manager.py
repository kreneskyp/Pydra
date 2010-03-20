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

import pydra
import pydra_settings
from pydra.cluster.module import Module
from pydra.cluster.auth.worker_avatar import WorkerAvatar
from pydra.cluster.constants import *
from pydra.cluster.module import Module
from pydra.cluster.tasks.task_manager import TaskManager

import logging
logger = logging.getLogger('root')


class WorkerManager(Module):

    _shared = ['master', 'workers', 'worker_connection_manager']

    def __init__(self):
        self._remotes = [
            ('MASTER',self.init_node),

            # worker proxy - functions exposed to the master that are for the
            # most part just passed through to the worker
            ('MASTER', self.kill_worker),
            ('MASTER', self.run_task),
            ('MASTER', self.stop_task),
            ('MASTER', self.worker_status),
            ('MASTER', self.task_status),
            ('MASTER', self.receive_results),
            ('MASTER', self.release_worker),
            ('MASTER', self.subtask_started),

            # master proxy - functions exposed to the workers that are passed
            # through to the Master
            ('WORKER', self.send_results),
            ('WORKER', self.request_worker),
            ('WORKER', self.worker_stopped),
            ('WORKER', self.request_worker_release)

        ]

        self._friends = {
            'task_manager' : TaskManager,
        }

        self._listeners = {
            'WORKER_CONNECTED':self.run_task_delayed,
            'WORKER_DISCONNECTED':self.clean_up_finished_worker
        }

        self.__lock = RLock()
        self.workers_finishing = []
        self.initialized = False


    def _register(self, manager):
        Module._register(self, manager)
        self.workers = {}


    def clean_up_finished_worker(self, worker):
        """
        Called when workers disconnect.  When a worker shuts itself down it
        becomes a zombie process (defunct) and while it does not use resources
        it does exist in the process table.  Just to keep things nice and tidy
        this function will kill any worker that disconnected and has the
        finished flag set
        """
        if worker.finished:
            # XXX If an error occurs while creating the worker process we won't
            # have a popen object. It also means we won't need to call wait()
            # for it to clean up
            if worker.popen:
                while True:
                    try:
                        worker.popen.wait()
                        break
                    except OSError:
                        logger.warn('Error cleaning up worker process, retrying')


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


    def kill_worker(self, master, worker_key, kill=False, fail=True):
        """
        Stops a worker process.

        @param worker_key: key of worker to kill
        @param kill: send SIGKILL instead of SIGTERM. Default is signal
                     is SIGTERM.  SIGKILL should only be used in extreme cases
        @param fail: workunit(s) should return as if they threw an exception.
                    Otherwise they will be marked as canceled.  Failed workunits
                    may be retried on other nodes depending on settings
        """
        with self.__lock:
            if not worker_key in self.workers:
                return
            
            worker = self.workers[worker_key]
            worker.finished = True
            logger.debug('Stopping %s with %s' % \
                        (worker_key, 'SIGKILL' if kill else 'SIGTERM'))

            # python >= 2.6 required for Popen.kill() and Popen.terminate()
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

        if fail:
            self.send_results(worker_key, 'Terminated by user', \
                              worker.workunit_key, failed=True)
        else:
            self.worker_stopped(worker_key)


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
        
        @param remote - remote function to call
        @param worker_id - id of worker this is proxied to
        """
        worker = self.workers[worker_id]
        return worker.remote.callRemote(remote, *args, **kwargs)


    def receive_results(self, master, *args, **kwargs):
        """
        Function called to make the subtask receive the results processed by
        another worker
        """
        return self.proxy_to_worker('receive_results', *args, **kwargs)


    def release_worker(self, master, worker_id, *args, **kwargs):
        """
        Releases a worker that was held by a task.
        """
        worker = self.workers[worker_id]
        worker.finished = True
        
        # send release on to worker.  must occur before worker is removed from
        # the pool.
        deferred = self.proxy_to_worker('release_worker', worker_id, *args, **kwargs)
        
        # emit WORKER_FINISHED to force cleanup of worker object.  Must occur
        # before response is sent to master.  Otherwise master could send a new
        # request before the worker is fully cleaned up
        self.emit('WORKER_FINISHED', worker)
        
        return deferred


    def request_worker(self, *args, **kwargs):
        return self.proxy_to_master('request_worker', *args, **kwargs)


    def request_worker_release(self, *args, **kwargs):
        return self.proxy_to_master('request_worker_release', *args, **kwargs)


    def retrieve_task_failed(self, *args, **kwargs):
        pass


    def run_task(self, avatar, worker_key, key, version, args={}, \
            workunits=None, main_worker=None, task_id=None):
        """
        Runs a task on this node.  This function should
        """
        self.task_manager.retrieve_task(key, version, self._run_task, \
                self.retrieve_task_failed, worker_key, args, workunits, \
                main_worker, task_id)


    def _run_task(self, key, version, task_class, module_search_path, \
            worker_key, args={}, workunits=None, main_worker=None, task_id=None):
        """
        Runs a task on this node.  The worker scheduler on master makes all
        decisions about which worker to run on.  This method only checks
        to see if the worker is already running or not.  This is a very
        explicit and deliberate division of repsonsibility.  Allowing
        decisions here dilutes the ability of the Master to control the
        cluster

        @param key - task key
        @param version - version of task to run
        @param task_class - task class to be created TODO why is this here?
        @param module_search_path - TODO why is this here?
        @param worker_key - key of worker to run task on
        @param args - args for task
        @param subtask_key - key of subtask to run
        @param workunit_key - key of workunit to run
        @param main_worker - main worker for this task
        @param task_id - id of task being run
        """
        logger.info('RunTask:%s  key=%s  sub=%s  main=%s' \
            % (task_id, key, workunits, main_worker))
        worker = None

        with self.__lock:
            if worker_key in self.workers:
                # worker exists.  reuse it.
                logger.debug('RunTask - Using existing worker %s' % worker_key)
                worker = self.workers[worker_key]
                worker.run_task_deferred = worker.remote.callRemote('run_task',\
                        key, version, args, workunits, main_worker, task_id)
            else:
                # worker not running. start it saving the information required
                # to start the subtask.  This function will return a deferred
                # to the master.  The deferred will be
                logger.debug('RunTask - Spawning worker: %s', worker_key)
                worker = WorkerAvatar(self.worker_connection_manager, \
                                                            worker_key)
                worker.worker_key = worker_key
                worker.run_task_deferred = Deferred()
                pydra_root = pydra.__file__[:pydra.__file__.rfind('/')]
                try:
                    worker.popen = Popen(['python',
                                '%s/cluster/worker/worker.py' % pydra_root,
                                worker_key,
                                pydra_settings.WORKER_PORT.__str__()])
                except OSError:
                    # XXX ocassionally processes will have a communcation error
                    # while loading.  The process will be running but the POpen
                    # object is not constructed.  This means that we have no
                    # access to the subprocess functions.  Instead we must get
                    # the pid from the newly run process after it starts.  The
                    # pid can then be used instead of the Popen object.
                    #
                    # relevant bugs:
                    #    http://pydra-project.osuosl.org/ticket/158
                    #    http://bugs.python.org/issue1068268
                    debug.warn('OSError while spawning process, failing back to pid. see ticket #158')
                    worker.run_task_deferred.addCallback(worker.get_pid)
                self.workers[worker_key] = worker

            worker.key = key
            worker.version = version
            worker.args = args
            worker.main_worker = main_worker
            worker.task_id=task_id
            if worker_key == main_worker:
                worker.local_workunits = workunits
            else:
                worker.workunits = workunits

            return worker.run_task_deferred


    def run_task_delayed(self, worker):
        """
        Callback when a worker has started.  start the intended task.  Attach
        the deferred originally returned in run_task to the deferred returned
        from worker.run_task.  This will cause the result to propagate through
        the deferreds back to master.
        """
        sent_deferred = worker.run_task_deferred
        deferred = self._run_task(worker.key, worker.version, None, None, \
                worker.worker_key, worker.args, worker.workunits, \
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
            if worker.main_worker == worker_key and not worker.local_workunits:
                self.emit('WORKER_FINISHED', worker)
                worker.finished = True

            else:
                # worker may be reused to clear all args to avoid confusion
                worker.workunits = None
                worker.local_workunits = None

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


    def subtask_started(self, master, worker, *args):
        """
        Called to inform the task that a queued subtask was started on a remote
        worker
        
        @param subtask - subtask path.
        @param id - database id for workunit.
        """
        return self.proxy_to_worker('subtask_started', worker, *args)


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


    def worker_stopped(self, worker, *args, **kwargs):
        with self.__lock:
            self.workers[worker].finished = True
        self.proxy_to_master('worker_stopped', worker, *args, **kwargs)
        return 1
