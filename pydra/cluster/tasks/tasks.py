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

from twisted.internet import reactor, threads\

from pydra.cluster.tasks import TaskNotFoundException,\
    STATUS_CANCELLED, STATUS_CANCELLED,STATUS_FAILED,STATUS_STOPPED,STATUS_RUNNING,\
    STATUS_PAUSED,STATUS_COMPLETE

import logging
logger = logging.getLogger('root')


class Task(object):
    """
    Task - class that wraps a set of functions as a runnable unit of work.  Once
    wrapped the task allows functions to be managed and tracked in a uniform way.

    This is an abstract class and requires the following functions to be implemented:
        * _work  -  does the work of the task
        * _reset - resets the state of the task when stopped
        * progress - returns the state of the task as an integer from 0 to 100
        * progressMessage - returns the state of the task as a readable string
    """
    parent = None
    _status = STATUS_STOPPED
    __callback = None
    _callbackargs = None
    workunit = None
    STOP_FLAG = False
    form = None

    msg = None
    description = 'Default description about Task baseclass.'


    def __eq__(self, val):
        return self.__repr__() == val.__repr__()


    def __init__(self, msg=None):
        self.msg = msg
        self.id = 1
        self.work_deferred = False


    def _complete(self, results):
        """
        Called by task when after _work() completes
        
        @param results - return value from work(...)
        """
        self._status = STATUS_COMPLETE

        if self.__callback:
            logger.debug('[%s] %s - Task._work() -Making callback' % (self.get_worker().worker_key, self))
            self.__callback(results, **self._callback_args)
        else:
            logger.warning('[%s] %s - Task._work() - NO CALLBACK TO MAKE: %s' % (self.get_worker().worker_key, self, self.__callback))


    def _generate_key(self):
        """
        Generate the key for this task using a recursive algorithm.
        """
        key = self.__class__.__name__
        base = self.parent.get_key()
        if base:
            key = '%s.%s' % (base, key)
        return key
    

    def _stop(self):
        """
        Stop the task.  This consists of just setting the STOP_FLAG.  The Task implementation
        will only stop if it honors the STOP_FLAG.  There is no safe way to kill the thread 
        running the work function.  This is because the Task might have implementation specific
        code for shutting down the task to enable a restart.  If we were to just kill the thread
        (which would be hard in twisted) it would end all processing and might stop in a wierd
        state.  Using the STOP_FLAG means you may have bad programs introduced to your cluster
        but for the time being it is the only choice
        """
        self.STOP_FLAG=True


    def _start(self, args={}, callback=None, callback_args={}):
        """
        overridden to prevent early task cleanup.  ParallelTask._work() returns immediately even though 
        work is likely running in the background.  There appears to be no effective way to block without 
        interupting twisted.Reactor.  The cleanup that normally happens in work() has been moved to
        task_complete() which will be called when there is no more work remaining.

        @param args - kwargs to be passed to _work
        @param callback - callback that will be called when work is complete
        @param callback_args - dictionary passed to callback as kwargs
        """
        self.__callback = callback
        self._callback_args=callback_args

        self._status = STATUS_RUNNING
        results = self._work(**args)

        return results


    def _work(self, **kwargs):
        """
        Calls the users work function and then calls the callback
        signally completion of this task.  This is split out from 
        do_work so that completion of work can be overridden to be
        asynchronous.  By default it will be synchronous and return 
        as soon as work() completes.  For any subclass that distributes
        work the callback cannot be called until after all workunits
        asynchronously return
        """
        results = self.work(**kwargs)
        self._complete(results)


    def get_key(self):
        """
        Get the key that represents this task instance.  This key will give
        the path required to find it if iterating from the root of the task.
        This is used so that subtasks can be selected.
        """
        return self._generate_key()


    def get_subtask(self, task_path):
        """
        Retrieves a subtask via task_path.  Task_path is the task_key
        split into a list for easier iteration
        """
        #A Task can't have children,  if this is the last entry in the path
        # then this is the right task
        if len(task_path) == 1 and task_path[0] == self.__class__.__name__:
            return self
        else:
            raise TaskNotFoundException("Task not found: %s" % task_path)


    def get_worker(self):
        """
        Retrieves the worker running this task.  This function is recursive and bubbles
        up through the task tree till the worker is reached
        """
        return self.parent.get_worker()


    def request_worker(self, *args, **kwargs):
        """
        Requests a worker for a subtask from the tasks parent.  calling this on any task will
        cause requests to bubble up to the root task whose parent will be the worker
        running the task.
        """
        return self.parent.request_worker(*args, **kwargs)


    def start(self, args={}, subtask_key=None, callback=None, callback_args={},
              errback=None, errback_args={}):
        """
        starts the task.  This will spawn the work in a workunit thread.
        """

        # only start if not already running
        if self._status == STATUS_RUNNING:
            return

        #if this was subtask find it and execute just that subtask
        if subtask_key:
            logger.debug('[%s] Task - starting subtask %s' % (self.get_worker().worker_key,subtask_key))
            split = subtask_key.split('.')
            subtask = self.get_subtask(split)
            subtask.logger = self.logger
            logger.debug('[%s] Task - got subtask'%self.get_worker().worker_key)
            self.work_deferred = threads.deferToThread(subtask._start, args, callback, callback_args)

        #else this is a normal task just execute it
        else:
            logger.debug('[%s] Task - starting task: %s' % (self.get_worker().worker_key,self))
            self.work_deferred = threads.deferToThread(self._start, args, callback, callback_args)

        if errback:
            self.work_deferred.addErrback(errback, **errback_args)

        return 1


    def status(self):
        """
        Returns the status of this task.  Used as a function rather than member variable so this
        function can be overridden
        """
        return self._status


    def work(self, **kwargs):
        """
        Function to be overrided by users to perform the real work
        """
        pass
