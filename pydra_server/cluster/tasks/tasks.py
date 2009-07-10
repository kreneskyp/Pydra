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
from pydra_server.util import deprecated
from pydra_server.cluster.tasks import TaskNotFoundException,\
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

    def _generate_key(self):
        """
        Generate the key for this task using a recursive algorithm.
        """
        key = self.__class__.__name__
        base = self.parent.get_key()
        if base:
            key = '%s.%s' % (base, key)

        return key


    def __init__(self, msg=None):
        self.msg = msg

        # TODO ticket #55 - replace with zope interface
        #_work = AbstractMethod('_work')
        #progress = AbstractMethod('progress')
        #progressMessage = AbstractMethod('progressMessage')
        #_reset = AbstractMethod('_reset')

        self.id = 1
        self.work_deferred = False

    def reset(self):
        """
        Resets the task, including setting the flags properly,  delegates implementation
        specific work to _reset()
        """
        self._status = STATUS_STOPPED
        self._reset()

    def start(self, args={}, subtask_key=None, callback=None, callback_args={}, errback=None):
        """
        starts the task.  This will spawn the work in a workunit thread.
        """

        # only start if not already running
        if self._status == STATUS_RUNNING:
            return

        #if this was subtask find it and execute just that subtask
        if subtask_key:
            logger.debug('Task - starting subtask %s - %s' % (subtask_key, args))
            split = subtask_key.split('.')
            subtask = self.get_subtask(split)
            logger.debug('Task - got subtask')
            self.work_deferred = threads.deferToThread(subtask.work, args, callback, callback_args)

        #else this is a normal task just execute it
        else:
            logger.debug('Task - starting task: %s' % args)
            self.work_deferred = threads.deferToThread(self.work, args, callback, callback_args)

        if errback:
            self.work_deferred.addErrback(errback)


        return 1


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


    def work(self, args={}, callback=None, callback_args={}):
        """
        Does the work of the task.  This is can be called directly for synchronous work or via start which
        causes a workunit thread to be spawned and call this function.  this method will set flags properly and
        delegate implementation specific work to _work(args)
        """
        logger.debug('%s - Task - in Task.work()'  % self.get_worker().worker_key)
        self._status = STATUS_RUNNING
        results = self._work(**args)
        self._status = STATUS_COMPLETE
        logger.debug('%s - Task - work complete' % self.get_worker().worker_key)

        self.work_deferred = None

        #make a callback, if any
        if callback:
            logger.debug('%s - Task - Making callback' % self)
            callback(results, **callback_args)
        else:
            logger.warning('%s - Task - NO CALLBACK TO MAKE: %s' % (self, self.__callback))

        return results


    def status(self):
        """
        Returns the status of this task.  Used as a function rather than member variable so this
        function can be overridden
        """
        return self._status


    def request_worker(self, task_key, args):
        """
        Requests a worker for a subtask from the tasks parent.  calling this on any task will
        cause requests to bubble up to the root task whose parent will be the worker
        running the task.
        """
        self.parent.request_worker(self, task_key, args)


    def get_worker(self):
        """
        Retrieves the worker running this task.  This function is recursive and bubbles
        up through the task tree till the worker is reached
        """
        return self.parent.get_worker()


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


    def __eq__(self, val):
        return self.__repr__() == val.__repr__()