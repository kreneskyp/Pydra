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

from twisted.internet import reactor

from pydra_server.cluster.constants import *
from pydra_server.cluster.module import Module
from pydra_server.cluster.tasks import ParallelTask
from pydra_server.logging import get_task_logger

# init logging
import logging
logger = logging.getLogger('root')


class WorkerTaskControls(Module):

    _shared = [
        'worker_key',
        'registry',
        'master',
        '_lock_connection',
    ]

    def __init__(self, manager):

        self._remotes = [
            ('MASTER', self.run_task),
            ('MASTER', self.stop_task),
            ('MASTER', self.status),
            ('MASTER', self.task_status),
            ('MASTER', self.receive_results),
            ('MASTER', self.return_work)
        ]

        Module.__init__(self, manager)

        self._lock = Lock()
        self.__task = None
        self.__task_instance = None
        self.__results = None
        self.__stop_flag = None
        self.__subtask = None
        self.__workunit_key = None
        self.__local_workunit_key = None
        self.__local_task_instance = None
        

    def run_task(self, key, args={}, subtask_key=None, workunit_key=None, \
        main_worker=None, task_id=None):
        """
        Runs a task on this worker
        """
        logger.info('[%s] RunTask:  key=%s  args=%s  sub=%s  w=%s  main=%s' \
            % (self.worker_key, key, args, subtask_key, workunit_key, \
            main_worker))

        # Register task with worker
        with self._lock:
            if not key:
                return "FAILURE: NO TASK KEY SPECIFIED"

            # is this task being run locally for the mainworker?
            run_local = subtask_key and self.worker_key == main_worker

            # Check to ensure this worker is not already busy.
            # The Master should catch this but lets be defensive.
            if run_local:
                busy =  not isinstance(self.__task_instance, ParallelTask) \
                        or self.__local_workunit_key

            else:
                busy = (self.__task and self.__task <> key) or self.__workunit_key
                
            if busy:
                logger.warn('Worker:%s - worker busy:  task=%s, instance=%s, local=%s:%s' \
                    % (self.worker_key, self.__task, self.__task_instance, \
                    run_local, self.__local_workunit_key))
                return "FAILURE THIS WORKER IS ALREADY RUNNING A TASK"


            # not busy.  set variables that mark this worker as busy.
            self.__task = key
            self.__subtask = subtask_key
            if run_local:
                logger.debug('[%s] RunTask: The master wants to exec a local work' % self.worker_key)
                self.__local_workunit_key = workunit_key
            else:
                self.__workunit_key = workunit_key


        # process args to make sure they are no longer unicode
        clean_args = {}
        if args:
            for arg_key, arg_value in args.items():
                clean_args[arg_key.__str__()] = arg_value

        # Create task instance and start it
        if run_local:
            self.__local_task_instance = object.__new__(self.registry[key])
            self.__local_task_instance.__init__()
            self.__local_task_instance.parent = self
            self.__local_task_instance.logger = get_task_logger( \
                self.worker_key, task_id, subtask_key, workunit_key)
            return self.__local_task_instance.start(clean_args, subtask_key, \
                self.work_complete, {'local':True}, errback=self.work_failed)

        else:
            #create an instance of the requested task
            self.__task_instance = object.__new__(self.registry[key])
            self.__task_instance.__init__()
            self.__task_instance.parent = self
            self.__task_instance.logger = get_task_logger(self.worker_key, \
                task_id)
            return self.__task_instance.start(clean_args, subtask_key, \
                self.work_complete, errback=self.work_failed)


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


    def work_complete(self, results, local=False):
        """
        Callback that is called when a job is run in non_blocking mode.
        """
        if local:    
            workunit_key = self.__local_workunit_key
            task_instance = self.__local_task_instance
            self.__local_workunit_key = None
        else:            
            workunit_key = self.__workunit_key
            task_instance = self.__task_instance
            self.__workunit_key = None

        if task_instance.STOP_FLAG:
            #stop flag, this task was canceled.
            with self._lock_connection:
                if self.master:
                    deferred = self.master.callRemote("stopped")
                    deferred.addErrback(self.send_stopped_failed)

                else:
                    self.__stop_flag = True

        else:
            #completed normally
            # if the master is still there send the results
            with self._lock_connection:
                if self.master:
                    deferred = self.master.callRemote("send_results", results, workunit_key)
                    deferred.addCallback(self.send_successful)
                    deferred.addErrback(self.send_results_failed, results, workunit_key)

                # master disapeared, hold results until it requests them
                else:
                    self.__results = results


    def work_failed(self, results):
        """
        Callback that there was an exception thrown by the task
        """
        self.__task = None

        with self._lock_connection:
            if self.master:
                deferred = self.master.callRemote("task_failed", results, self.__workunit_key)
                logger.error('Worker - Task Failed: %s' % results)
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
        with self._lock_connection:
            #check again for master.  the lock is released so its possible
            #master could connect and be set before we set the flags indicating 
            #the problem
            if self.master:
                # reconnected, just resend the call.  The call is recursive from this point
                # if by some odd chance it disconnects again while sending
                logger.error('[%s] results failed to send but Node is still here' % self.worker_key)
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
        with self._lock_connection:
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


    def send_successful(self, results):
        """
        Generic callback for when send methods are successful.  This method
        cleans up and shuts down the worker
        """
        self.emit('WORKER_FINISHED')
        reactor.stop()


    def task_status(self):
        """
        Returns status of task this task is performing
        """
        if self.__task_instance:
            return self.__task_instance.progress()


    def receive_results(self, worker_key, results, subtask_key, workunit_key):
        """
        Function called to make the subtask receive the results processed by another worker
        """
        logger.info('Worker:%s - received REMOTE results for: %s' % (self.worker_key, subtask_key))
        subtask = self.__task_instance.get_subtask(subtask_key.split('.'))
        subtask.parent._work_unit_complete(results, workunit_key)
        

    def release_worker(self):
        """
        Function called by Main Workers to release a worker.  This does not
        specify which worker to release because the main worker does not know
        which worker is optimal to release if there is a choice.
        """
        self.master.callRemote('release_worker')


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


    def get_key(self):
        """
        recursive task key generation function.  This stops the recursion
        """
        return None    

