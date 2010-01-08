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

import simplejson
from twisted.internet import reactor, threads

from pydra.cluster.constants import *
from pydra.cluster.module import Module
from pydra.cluster.tasks import ParallelTask, MapReduceTask
from pydra.cluster.tasks.task_manager import TaskManager
from pydra.logs import get_task_logger

# init logging
import logging
logger = logging.getLogger('root')


class WorkerTaskControls(Module):

    _shared = [
        'worker_key',
        'master',
        '_lock_connection',
    ]

    def __init__(self):

        self._remotes = [
            ('MASTER', self.run_task),
            ('MASTER', self.stop_task),
            ('MASTER', self.status),
            ('MASTER', self.task_status),
            ('MASTER', self.receive_results),
            ('MASTER', self.release_worker),
            ('MASTER', self.return_work)
        ]

        self._friends = {
            'task_manager' : TaskManager,
        }

        self._lock = Lock()
        self.__task = None
        self.__task_instance = None
        self.__results = None
        self.__stop_flag = None
        self.__subtask = None
        self.__workunit = None

        # shutdown tracking
        self.__pending_releases = 0
        self.__pending_shutdown = False

    def run_task(self, key, version, args={}, subtask_key=None, workunit_key=None, \
            main_worker=None, task_id=None):
        self.task_manager.retrieve_task(key, version,
                self._run_task, self.retrieve_task_failed, args, subtask_key,
                workunit_key, main_worker, task_id)


    def _run_task(self, key, version, task_class, module_search_path, args={},
            subtask_key=None, workunit=None, main_worker=None, task_id=None):
        """
        Runs a task on this worker
        
        @param key - key identifying task to run
        @param version - version of task to run.
        @param task_class - class instance of the task
        @param module_search_path - ????????????????
        @param args - kwargs that will be passed to the Task.start()
        @param subtask_key - key identifying subtask to run
        @param workunit - key to data, or data to processed
        @param main_worker - key for the main worker of this task
        @param task_id - ID of the task instance
        """
        logger.info('RunTask:  key=%s  args=%s  sub=%s  w=%s  main=%s' \
            % (key, args, subtask_key, workunit, main_worker))

        # Register task with worker
        with self._lock:
            if not key:
                return "FAILURE: NO TASK KEY SPECIFIED"

            # save what worker is running
            self.__task = key
            self.__subtask = subtask_key

        # process args to make sure they are no longer unicode.  This is an
        # issue with the args coming through the django frontend.
        clean_args = {}
        args = simplejson.loads(args)
        if args:
            for arg_key, arg_value in args.items():
                clean_args[arg_key.__str__()] = arg_value


        # only create a new task instance if this is the root task.  Otherwise
        # subtasks will be created within the structure of the task.
        if not self.__task_instance:
            self.__task_instance = task_class()
            self.__task_instance.parent = self
            self.__task_instance.logger = get_task_logger(self.worker_key, \
                task_id)

        # start the task.  If this is actually a subtask, then the task is
        # responsible for starting the subtask instead of the main task
        return self.__task_instance.start(clean_args, subtask_key, \
                        callback=self.work_complete,
                        callback_args = {'workunit':workunit},
                        errback=self.work_complete,
                        errback_args={'workunit':workunit, 'failed':True}) 


    def stop_task(self):
        """
        Stops the current task.
        """
        logger.info('Received STOP command')
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


    def work_complete(self, results, workunit=None, failed=False):
        """
        Callback that is called when a job is run in non_blocking mode and has
        finished.  This callback handles both successful tasks and failures
        caused by exceptions in the users task.
        
        @param results - results from task, or a twisted failure object
        @param local - is this workunit being processed locally by the main
                        worker
        @param failed - was there an exception thrown in the task
        """
        
        # create traceback if its an error
        if failed:
            results = results.__str__()

        if self.__task_instance.STOP_FLAG:
            # If stop flag is set for either the main task or local task
            # then ignore any results and stop the task
            with self._lock_connection:
                if self.master:
                    deferred = self.master.callRemote("worker_stopped")
                    deferred.addCallback(self.send_successful)
                    deferred.addErrback(self.send_stopped_failed)

                else:
                    self.__stop_flag = True

        else:
            #completed normally
            # if the master is still there send the results
            with self._lock_connection:
                if self.master:
                    deferred = self.master.callRemote("send_results", results, \
                                                      workunit, failed)
                    deferred.addCallback(self.send_successful)
                    deferred.addErrback(self.send_results_failed, results, \
                                        workunit)

                # master disapeared, hold results until it requests them
                else:
                    self.__results = results


    def send_results_failed(self, results, task_results, workunit):
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
                logger.error('results failed to send but Node is still here')
                #deferred = self.master.callRemote("send_results", task_results, task_results)
                #deferred.addErrback(self.send_results_failed, task_results, task_results)

            else:
                #nope really isn't connected.  set flag.  even if connection is in progress
                #this thread has the lock and reconnection cant finish till we release it
                self.__results = task_results
                self.__workunit = workunit


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
        if (results):
            threads.deferToThread(self.shutdown)


    def task_status(self):
        """
        Returns status of task this task is performing
        """
        if self.__task_instance:
            return self.__task_instance.progress()


    def receive_results(self, worker_key, results, subtask_key, workunit_key):
        """
        Function called to make the subtask receive the results processed by
        another worker.  This call is ignored if STOP flag is already set.
        """
        if not self.__task_instance.STOP_FLAG:
            logger.info('received REMOTE results for: %s' % subtask_key)
            subtask = self.__task_instance.get_subtask(subtask_key.split('.'))
            subtask.parent._work_unit_complete(results, workunit_key)


    def release_worker(self):
        """
        called be the Node/Master to inform this worker that it is released
        and may shutdown
        """
        threads.deferToThread(self.shutdown)


    def shutdown(self):
        with self._lock:
            if self.__pending_releases:
                self.__pending_shutdown = True
                return

        logger.debug('Released, shutting down')
        self.emit('WORKER_FINISHED')
        reactor.stop()


    def request_worker(self, subtask_key, args, workunit_key):
        """
        Requests a work unit be handled by another worker in the cluster
        """
        logger.info('requesting worker for: %s' % subtask_key)
        deferred = self.master.callRemote('request_worker', subtask_key, args, workunit_key)


    def request_worker_release(self):
        """
        Function called by Main Workers to release a worker.  This does not
        specify which worker to release because the main worker does not know
        which worker is optimal to release if there is a choice.
        """
        with self._lock:
            self.__pending_releases += 1
            deferred = self.master.callRemote('request_worker_release')
            deferred.addCallback(self.release_request_successful)


    def release_request_successful(self, results):
        """
        A worker release request was successful
        """
        with self._lock:
            self.__pending_releases -= 1
            if self.__pending_shutdown and self.__pending_releases == 0:
                threads.deferToThread(self.shutdown)


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


    def retrieve_task_failed(self, task_key, version, err):
        pass

