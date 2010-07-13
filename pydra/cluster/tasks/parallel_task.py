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

import logging
from threading import Thread, RLock
from twisted.internet import reactor, threads

from pydra.cluster.tasks import Task, TaskNotFoundException, STATUS_CANCELLED,\
    STATUS_FAILED,STATUS_STOPPED,STATUS_RUNNING,STATUS_PAUSED,STATUS_COMPLETE
from pydra.cluster.tasks.datasource import unpack, validate
from pydra.util.key import thaw

class ParallelTask(Task):
    """
    ParallelTask - is a task that can be broken into discrete work units
    """
    _data_in_progress = {}      # workunits of data
    _workunit_count = 0         # count of workunits handed out.  This is used to identify transactions
    _workunit_total = 0
    _workunit_completed = 0     # count of workunits handed out.  This is used to identify transactions
    subtask_key = None          # cached key from subtask

    datasource = None
    slicer = None

    def __init__(self, msg=None):
        Task.__init__(self, msg)
        self._lock = RLock()             # general lock
        self.subtask = None              # subtask that is parallelized
        self.__subtask_class = None      # class of subtask
        self.__subtask_args = None       # args for initializing subtask
        self.__subtask_kwargs = None     # kwargs for initializing subtask

        self.datasource = validate(self.datasource)

        self.logger = logging.getLogger('root')


    def __getattribute__(self, key):
        """
        Overridden to lazy instantiate subtask when requested
        """
        if key == 'subtask':
            if not self.__dict__['subtask']:
                subtask = self.__subtask_class(*self.__subtask_args, \
                                                    **self.__subtask_kwargs)
                self.subtask = subtask
            return self.__dict__['subtask']
        return Task.__getattribute__(self, key)


    def __setattr__(self, key, value):
        Task.__setattr__(self, key, value)
        if key == 'subtask' and value:
            value.parent = self


    def _get_subtask(self, task_path, clean=False):
        """
        Returns the subtask specified by the path.  Overridden to search subtask
        of this class.  This function lazily loads the subtask if it is not
        already instantiated.
        
        @param task_path - list of strings that correspond to a task's location
                           within a task heirarchy
        @param clean - a new (clean) instance of the task is requested.
        
        @returns a tuple containing the consumed portion of the task path and
                    the task matches the request.
        """
        if len(task_path) == 1:
            if task_path[0] == self.__class__.__name__:
                return task_path, self
            else:
                raise TaskNotFoundException("Task not found: %s" % task_path)
        #recurse down into the child
        consumed, subtask = self.subtask._get_subtask(task_path[1:])
        return task_path[:2], subtask


    def _request_workers(self):
        """
        Create work requests for all planned subtasks.

        This function eagerly creates all planned work requests in one shot,
        using `get_work_units()` to create all work units.

        More complex `Task` subclasses, like `MapReduceTask`, may employ a
        more sophisticated algorithm that permits cross worker dependencies.
        """

        for data, index in self.get_work_units():
            self.logger.debug('Paralleltask - assigning remote work: key=%s, args=%s'
                % ('--', index))
            self.parent.request_worker(self.subtask.get_key(), {'data': data},
                index)


    def _stop(self):
        """
        Overridden to call stop on all children
        """
        Task._stop(self)
        self.subtask._stop()


    def _work(self, **kwargs):
        """
        Work function overridden to delegate workunits to other Workers.
        """
        # request initial workers
        self._request_workers()
        self.logger.debug('Paralleltask - initial work assigned!')


    def _batch_complete(self, results):
        for workunit, result, failed in results:
            if not failed:
                self._work_unit_complete(results, workunit)


    def _work_unit_complete(self, results, index):
        """
        A work unit completed.  Handle the common management tasks to remove the data
        from in_progress.  Also call task specific work_unit_complete(...)

        This method *MUST* lock while it is altering the lists of data
        """
        self.logger.debug('Paralleltask - Work unit completed')
        with self._lock:
            # run the task specific post process
            self.work_unit_complete(self._data_in_progress[index], results)

            # remove the workunit from _in_progress
            del self._data_in_progress[index]

            #check stop flag
            if self.STOP_FLAG:
                self.task_complete(None)

            # process any new work requests.  This moves any data that can be
            # queued into the queue, and into the in_progress list locally
            self._request_workers()

            # no data left in progress, release 1 worker.  when there is work in
            # the queue the waiting worker will be selected automatically by
            # the scheduler.  Releasing it must be explicit though.
            if not self._data_in_progress:
                self.logger.debug('ParallelTask - releasing a worker')
                self.get_worker().request_worker_release()

            self._workunit_completed += 1

            #check for more work
            if not self._data_in_progress:
                #all work is done, call the task specific function to combine the results 
                self.logger.debug('Paralleltask - all workunits complete, calling task post process')
                results = self.work_complete()
                self._complete(results)
                return


    def _worker_failed(self, index):
        """
        A worker failed while working.  re-add the data to the list
        """
        self.logger.warning('Paralleltask - Worker failure during workunit')
        with self._lock:
            #remove data from in progress
            data = self._data_in_progress[index]
            del self._data_in_progress[index]


    @staticmethod
    def from_subtask(cls, *args, **kwargs):
        """
        Creates a new ParallelTask with the specified class, args, and
        kwargs as its subtask.
        """
        pt = ParallelTask()
        pt.set_subtask(cls, *args, **kwargs)
        return pt


    def get_work_units(self):
        """
        Yield a series of work units.

        This function returns *all* work units, one by one. For each work
        unit, the data of the unit is stored in `_data_in_progress`.

        Warning: This method will take the instance lock as needed, but should
        not be locked during yields.

        :return: tuple(data, index)
        """
        # XXX needs to have a delayable path as well
        slicer = unpack(self.datasource)

        while True:
            data, index = next(slicer), self._workunit_count

            yield data, index

            with self._lock:
                self._workunit_count += 1
                self._data_in_progress[index] = data


    def progress(self):
        """
        progress - returns the progress as a number 0-100.

        A parallel task's progress is a derivitive of its workunits:
           COMPLETE_WORKUNITS / TOTAL_WORKUNITS
        """
        total = self._workunit_completed + len(self._data_in_progress)

        if total == 0:
            return 0

        return 100 * self._workunit_completed  / total


    def set_subtask(self, class_, *args, **kwargs):
        """
        Sets the subtask for this paralleltask.  The class, args, and kwargs
        are stored so that they may be lazily instantiated when needed.
        """
        self.__subtask_class = class_
        self.__subtask_args = args
        self.__subtask_kwargs = kwargs


    def start_subtask(self, task, subtask_key, workunit, kwargs, callback, \
                      callback_args):
        """
        Overridden to retrieve input arguments from datastore self.input.  These
        values are added to kwargs and passed to the subtask.  values in the
        workunit take preference over values in kwargs
        """
        # TODO: code after datasource update
        #args, kwargs = self.input.unpack(workunit, (), kwargs)
        #task._start(args, kwargs, callback, callback_args)
        task._start(workunit, callback, callback_args)


    def work_complete(self):
        """
        Method stub for method called to post process completion of task.  This
        must be overridden by users for their task specific work
        """
        pass


    def work_unit_complete(self, workunit, results):
        """
        Method stub for method called to post process results.  This
        is implemented by users that want to include automatic post-processing

        @param workunit - key and other args sent when assigning the workunit
        @param results - results sent by the completed subtask
        """
        pass
