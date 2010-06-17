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

from twisted.internet import reactor, threads

from pydra.cluster.tasks import TaskNotFoundException,\
    STATUS_CANCELLED,STATUS_FAILED,STATUS_STOPPED,STATUS_RUNNING,\
    STATUS_PAUSED,STATUS_COMPLETE

import logging
from pydra.logs.logger import get_task_logger
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
            self.logger.debug('%s - Task._work() -Making callback' % self)
            self.__callback(results, **self._callback_args)
        else:
            self.logger.warning('%s - Task._work() - NO CALLBACK TO MAKE: %s'
                % (self, self.__callback))
    

    def _get_subtask(self, task_path, clean=False):
        """
        Task baseclass specific logic for retrieving a subtask.  The key might
        correspond to this class instead of it's children.  The default
        implementation of this function only checks self.
        
        If the task does not match a TaskNotFoundException should be raised
        
        @param task_path - list of strings that correspond to a task's location
                           within a task heirarchy
        @param clean - a new (clean) instance of the task is requested.
        
        @returns a tuple containing the consumed portion of the task path and
                    the task matches the request.
        """
        #A Task can't have children,  if this is the last entry in the path
        # then this is the right task
        if len(task_path) == 1 and task_path[0] == self.__class__.__name__:
            return task_path, self
        else:
            raise TaskNotFoundException("Task not found: %s" % task_path)


    def _stop(self):
        """
        Stop the task.  This consists of just setting the STOP_FLAG.
        The Task implementation will only stop if it honors the STOP_FLAG. There
        is no safe way to kill the thread running the work function.  This is
        because the Task might have implementation specific code for shutting
        down the task to enable a restart.  If we were to just kill the thread
        (which would be hard in twisted) it would end all processing and might
        stop in a wierd state.  Using the STOP_FLAG means you may have bad
        programs introduced to your cluster but for the time being it is the
        only choice
        
        this method should be overridden to stop subtasks if a descendent of
        this class contains them
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
        return results


    def get_key(self):
        """
        A unique key that represents this task instance.

        This key can be used to find this task from its root. The primary
        purpose of these keys are to find subtasks.
        """
        key = self.__class__.__name__
        base = self.parent.get_key()
        if base:
            key = '%s.%s' % (base, key)
        return key


    def get_subtask(self, task_path, clean=False):
        """
        Returns a subtask from a task_path.  This function drills down through
        a task heirarchy to retrieve the requested task.  This method calls
        Task._get_subtask(...) to
        
        @param task_path - list of strings that correspond to a task's location
                           within a task heirarchy
        @param clean - returns a clean instance of the requested subtask.  This
            does nothing for a Task, which has no subtasks.  In this scenario it
            is easier to construct a new instance of by calling the constructor.
            
            This also only affects the task to be returned.  Other tasks in the
            heirarchy are only instantiated as needed to recurse to the
            requested task.

        @returns requested task, or TaskNotFoundException
        """
        subtask = self
        while task_path:
            task = subtask
            consumed_path, subtask = task._get_subtask(task_path)
            task_path = task_path[len(consumed_path):]
        if clean and not subtask._status == STATUS_STOPPED:
            task_path, subtask = task._get_subtask(consumed_path, clean=True)
        return subtask


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


    def start(self, args={}, subtask_key=None, workunit=None, task_id=-1, \
              callback=None, callback_args={}, errback=None, errback_args={}):
        """
        starts the task.  This will spawn the work in a separate thread.
        
        @param args - arguments to pass to task
        @param subtask_key - subtask to run
        @param workunit - key of workunit or data of workunit
        @param task_id - id of task being run
        @param callback - callback to execute after task is complete
        @param callback_args - args to pass to callback
        @param errback - call back to execute if there is an exception
        @param errback_args - arguments to pass to errback
        """

        #if this was subtask find it and execute just that subtask
        if subtask_key:
            self.logger.debug('Task - starting subtask %s' % subtask_key)
            split = subtask_key.split('.')
            subtask = self.get_subtask(split, True)
            subtask.logger = get_task_logger(self.get_worker().worker_key, \
                                             task_id, \
                                             subtask_key, workunit)
            self.logger.debug('Task - got subtask')
            self.work_deferred = threads.deferToThread(subtask._start, args, \
                                            callback, callback_args)

        elif self._status == STATUS_RUNNING:
            # only start root task if not already running
            return
        
        else:
            #else this is a normal task just execute it
            self.logger.debug('Task - starting task: %s' % self)
            self.work_deferred = threads.deferToThread(self._start, args, callback, callback_args)

        if errback:
            self.work_deferred.addErrback(errback, **errback_args)

        return 1


    def start_subtask(self, subtask, args, workunit, task_id, callback, \
                      callback_args):
        """
        Starts a subtask.  This functions sets additional parameters needed by
        a subtask such as workunit selection.
        
        This method should be overridden by subclasses of Task that wish to
        include workunits or other subtask specific functionality.  This method
        sets up logging for the subtask so any overridden function should likely
        call super as well.
        
        @param args - arguments to pass to task
        @param subtask - key of subtask to run
        @param workunit - key of workunit or data of workunit
        @param task_id - id of task being run
        @param callback - callback to execute after task is complete
        @param callback_args - args to pass to callback
        """
        pass

    
    def subtask_started(self, subtask, id):
        """
        Called to inform the task that a queued subtask was started on a remote
        worker
        
        @param subtask - subtask path.
        @param id - id for workunit.
        """
        self.logger.info('*** Workunit Started - %s:%s ***' % (subtask, id))


    def status(self):
        """
        Returns the status of this task.  Used as a function rather than member variable so this
        function can be overridden
        """
        return self._status


    def work(self, *args, **kwargs):
        """
        Do the actual computation of the task.

        This method is abstract and must be implemented by any subclasses.
        """
        raise NotImplementedError


    def progress(self):
        """
        Return the current progress of the task, as an integer between 0 and
        100.

        The default implementation returns 100 if the task is finished and 0
        otherwise.
        """
        return 100 if self.status == STATUS_COMPLETE else 0
