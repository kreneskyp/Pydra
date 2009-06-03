#! /usr/bin/python

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

#
# Setup django environment
#
if __name__ == '__main__':
    import sys
    import os

    #python magic to add the current directory to the pythonpath
    sys.path.append(os.getcwd())

    #
    if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
        os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

import os, sys
from twisted.spread import pb
from twisted.internet import reactor
from twisted.cred import credentials
from twisted.internet.protocol import ReconnectingClientFactory
from threading import Lock

from pydra_server.cluster.auth.rsa_auth import RSAClient, load_crypto
from pydra_server.cluster.tasks.task_manager import TaskManager
from constants import *


# init logging
import settings
from pydra_server.logging.logger import init_logging
logger = init_logging(settings.LOG_FILENAME_NODE)


class MasterClientFactory(pb.PBClientFactory):
    """
    Subclassing of PBClientFactory to add automatic reconnection
    """
    def __init__(self, reconnect_func, *args, **kwargs):
        pb.PBClientFactory.__init__(self)
        self.reconnect_func = reconnect_func
        self.args = args
        self.kwargs = kwargs

    def clientConnectionLost(self, connector, reason):
        logger.warning('Lost connection to master.  Reason:', reason)
        pb.PBClientFactory.clientConnectionLost(self, connector, reason)
        self.reconnect_func(*(self.args), **(self.kwargs))

    def clientConnectionFailed(self, connector, reason):
        logger.warning('Connection to master failed. Reason:', reason)
        pb.PBClientFactory.clientConnectionFailed(self, connector, reason)




class Worker(pb.Referenceable):
    """
    Worker - The Worker is the workhorse of the cluster.  It sits and waits for Tasks and SubTasks to be executed
            Each Task will be run on a single Worker.  If the Task or any of its subtasks are a ParallelTask 
            the first worker will make requests for work to be distributed to other Nodes
    """
    def __init__(self, master_host, master_port, node_key, worker_key):
        self.id = id
        self.__task = None
        self.__task_instance = None
        self.__results = None
        self.__stop_flag = None
        self.__lock = Lock()
        self.__lock_connection = Lock()
        self.master = None
        self.master_host = master_host
        self.master_port = master_port
        self.node_key = node_key
        self.worker_key = worker_key
        self.reconnect_count = 0

        # load crypto for authentication
        # workers use the same keys as their parent Node
        self.pub_key, self.priv_key = load_crypto('./node.key')
        self.master_pub_key = load_crypto('./node.master.key', False)
        self.rsa_client = RSAClient(self.priv_key)

        #load tasks that are cached locally
        self.task_manager = TaskManager()
        self.task_manager.autodiscover()
        self.available_tasks = self.task_manager.registry

        logger.info('Started Worker: %s' % worker_key)
        self.connect()

    def connect(self):
        """
        Make initial connections to all Nodes
        """
        import fileinput

        logger.info('worker:%s - connecting to master @ %s:%s' % (self.worker_key, self.master_host, self.master_port))
        factory = MasterClientFactory(self.reconnect)
        reactor.connectTCP(self.master_host, self.master_port, factory)

        deferred = factory.login(credentials.UsernamePassword(self.worker_key, '1234'), client=self)
        deferred.addCallbacks(self.connected, self.reconnect, errbackArgs=("Failed to Connect"))

    def reconnect(self, *arg, **kw):
        with self.__lock_connection:
            self.master = None
        reconnect_delay = 5*pow(2, self.reconnect_count)
        #let increment grow exponentially to 5 minutes
        if self.reconnect_count < 6:
            self.reconnect_count += 1 
        logger.debug('worker:%s - reconnecting in %i seconds' % (self.worker_key, reconnect_delay))
        self.reconnect_call_ID = reactor.callLater(reconnect_delay, self.connect)

    def connected(self, result):
        """
        Callback called when connection to master is made
        """
        with self.__lock_connection:
            self.master = result
        self.reconnect_count = 0

        logger.info('worker:%s - connected to master @ %s:%s' % (self.worker_key, self.master_host, self.master_port))

        # Authenticate with the master
        self.rsa_client.auth(result, self.master_pub_key.encrypt)


    def connect_failed(self, result):
        """
        Callback called when conenction to master fails
        """
        self.reconnect()

    def run_task(self, key, args={}, subtask_key=None, workunit_key=None, available_workers=1):
        """
        Runs a task on this worker
        """

        #Check to ensure this worker is not already busy.
        # The Master should catch this but lets be defensive.
        with self.__lock:
            if self.__task:
                return "FAILURE THIS WORKER IS ALREADY RUNNING A TASK"
            self.__task = key
            self.__subtask = subtask_key
            self.__workunit_key = workunit_key

        self.available_workers = available_workers

        logger.info('Worker:%s - starting task: %s:%s  %s' % (self.worker_key, key,subtask_key, args))
        #create an instance of the requested task
        self.__task_instance = object.__new__(self.available_tasks[key])
        self.__task_instance.__init__()
        self.__task_instance.parent = self

        # process args to make sure they are no longer unicode
        clean_args = {}
        if args:
            for key, arg in args.items():
                clean_args[key.__str__()] = arg

        return self.__task_instance.start(clean_args, subtask_key, self.work_complete, errback=self.work_failed)


    def stop_task(self):
        """
        Stops the current task
        """
        logger.info('%s - Received STOP command' % self.worker_key)
        if self.__task_instance:
            self.__task_instance._stop()


    def status(self):
        """
        Return the status of the current task if running, else None
        """
        # if there is a task it must still be running
        if self.__task:
            return (WORKER_STATUS_WORKING, self.__task, self.__subtask)

        # if there are results it was waiting for the master to retrieve them
        if self.__results:
            return (WORKER_STATUS_FINISHED, self.__task, self.__subtask)

        return (WORKER_STATUS_IDLE,)


    def work_complete(self, results):
        """
        Callback that is called when a job is run in non_blocking mode.
        """
        stop_flag = self.__task_instance.STOP_FLAG
        self.__task = None

        if stop_flag:
            #stop flag, this task was canceled.
            with self.__lock_connection:
                if self.master:
                    deferred = self.master.callRemote("stopped")
                    deferred.addErrback(self.send_stopped_failed)
                else:
                    self.__stop_flag = True

        else:
            #completed normally
            # if the master is still there send the results
            with self.__lock_connection:
                if self.master:
                    deferred = self.master.callRemote("send_results", results, self.__workunit_key)
                    deferred.addErrback(self.send_results_failed, results, self.__workunit_key)

                # master disapeared, hold results until it requests them
                else:
                    self.__results = results


    def work_failed(self, results):
        """
        Callback that there was an exception thrown by the task
        """
        self.__task = None

        with self.__lock_connection:
            if self.master:
                deferred = self.master.callRemote("failed", results, self.__workunit_key)
                #deferred.addErrback(self.send_failed_failed, results, self.__workunit_key)

            # master disapeared, hold failure until it comes back online and requests it
            else:
                #TODO implement me
                pass


    def send_results_failed(self, results, task_results, workunit_key):
        """
        Errback called when sending results to the master fails.  resend when
        master reconnects
        """
        with self.__lock_connection:
            #check again for master.  the lock is released so its possible
            #master could connect and be set before we set the flags indicating 
            #the problem
            if self.master:
                # reconnected, just resend the call.  The call is recursive from this point
                # if by some odd chance it disconnects again while sending
                logger.error('results failed to send by master is still here')
                #deferred = self.master.callRemote("send_results", task_results, task_results)
                #deferred.addErrback(self.send_results_failed, task_results, task_results)

            else:
                #nope really isn't connected.  set flag.  even if connection is in progress
                #this thread has the lock and reconnection cant finish till we release it
                self.__results = task_results
                self.__workunit_key = workunit_key


    def send_stopped_failed(self, results):
        """
        failed to send the stopped message.  set the flag and wait for master to reconnect
        """
        with self.__lock_connection:
            #check again for master.  the lock is released so its possible
            #master could connect and be set before we set the flags indicating 
            #the problem
            if master:
                # reconnected, just resend the call.  The call is recursive from this point
                # if by some odd chance it disconnects again while sending
                logger.error('STOP FAILED BUT MASTER STILL HERE')
                #deferred = self.master.callRemote("stopped")
                #deferred.addErrBack(self.send_stopped_failed)

            else:
                #nope really isn't connected.  set flag.  even if connection is in progress
                #this thread has the lock and reconnection cant finish till we release it
                self.__stop_flag = True


    def task_status(self):
        """
        Returns status of task this task is performing
        """
        if self.__task_instance:
            return self.__task_instance.progress()


    def receive_results(self, results, subtask_key, workunit_key):
        """
        Function called to make the subtask receive the results processed by another worker
        """
        subtask = self.__task_instance.get_subtask(subtask_key.split('.'))
        subtask.parent._work_unit_complete(results, workunit_key)


    def request_worker(self, subtask_key, args, workunit_key):
        """
        Requests a work unit be handled by another worker in the cluster
        """
        logger.info('Worker:%s - requesting worker for: %s' % (self.worker_key, subtask_key))
        deferred = self.master.callRemote('request_worker', subtask_key, args, workunit_key)


    def return_work(self, subtask_key, workunit_key):
        subtask = self.__task_instance.get_subtask(subtask_key.split('.'))
        subtask.parent._worker_failed(workunit_key)


    def get_worker(self):
        """
        Recursive function so tasks can find this worker
        """
        return self

    def remote_status(self):
        return self.status()

    def remote_task_list(self):
        return self.available_tasks.keys()

    def remote_run_task(self, key, args={}, subtask_key=None, workunit_key=None, available_workers=1):
        return self.run_task(key, args, subtask_key, workunit_key, available_workers)

    def remote_stop_task(self):
        return self.stop_task()

    def remote_receive_results(self, results, subtask_key, workunit_key):
        return self.receive_results(results, subtask_key, workunit_key)

    def remote_return_work(self, subtask_key, workunit_key):
        """
        Called by the Master when a Worker disconnected while working on a task.
        The work unit is returned so another worker can finish it.
        """
        return self.return_work(subtask_key, workunit_key)

    def remote_task_status(self):
        return self.task_status()


if __name__ == "__main__":
    master_host = sys.argv[1]
    master_port = int(sys.argv[2])
    node_key    = sys.argv[3]
    worker_key  = sys.argv[4]

    worker = Worker(master_host, master_port, node_key, worker_key)
    reactor.run()
