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
from threading import Thread, Lock
from twisted.internet import reactor, threads

from pydra_server.cluster.tasks import Task, TaskNotFoundException, STATUS_CANCELLED, STATUS_CANCELLED,\
    STATUS_FAILED,STATUS_STOPPED,STATUS_RUNNING,STATUS_PAUSED,STATUS_COMPLETE

import logging
logger = logging.getLogger('root')

class ParallelTask(Task):
    """
    ParallelTask - is a task that can be broken into discrete work units
    """
    _lock = None                # general lock
    #_available_workers = 1      # number of workers available to this task
    _data = None                # list of data for this task
    _data_in_progress = {}      # workunits of data
    _workunit_count = 0         # count of workunits handed out.  This is used to identify transactions
    subtask = None              # subtask that is parallelized
    subtask_key = None          # cached key from subtask

    def __init__(self, msg=None):
        Task.__init__(self, msg)
        self._lock = Lock()

    def __setattr__(self, key, value):
        Task.__setattr__(self, key, value)
        if key == 'subtask':
            value.parent = self

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


    def progress(self):
        """
        progress - returns the progress as a number 0-100.

        A parallel task's progress is a derivitive of its workunits:
           COMPLETE_WORKUNITS / TOTAL_WORKUNITS
        """
        return -1


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

        # save data, if any
        if kwargs and kwargs.has_key('data'):
            self._data = kwargs['data']
            logger.debug('Paralleltask - data was passed in!')


        # expand all the work units eagerly. the master will handles these
        # worker requests. other task implementations (like MapReduceTask) may
        # employ a more sophisticated mechanism that allows dependence between
        # work units.
        data, index = self.get_work_unit()
        while data is not None:
            logger.debug('Paralleltask - assigning remote work')
            self.parent.request_worker(self.subtask.get_key(), {'data':data}, index)
            data, index = self.get_work_unit()

        logger.debug('Paralleltask - initial work assigned')


    def get_subtask(self, task_path):
        if len(task_path) == 1:
            if task_path[0] == self.__class__.__name__:
                return self
            else:
                raise TaskNotFoundException("Task not found: %s" % task_path)

        #pop this class off the list
        task_path.pop(0)

        #recurse down into the child
        return self.subtask.get_subtask(task_path)


    def _assign_work(self, local=False):
        """
        assign a unit of work to a Worker by requesting a worker from the compute cluster
        """
        logger.critical('FIX ME, i should send any new work requests or release worker')
        data, index = self.get_work_unit()
        if not data == None:
            if local:
                logger.debug('Paralleltask - starting work locally')
                self.subtask.start({'data':data}, callback=self._work_unit_complete, callback_args={'index':index, 'local':True})

            else:
                logger.debug('Paralleltask - assigning remote work')
                self.parent.request_worker(self.subtask.get_key(), {'data':data}, index)

        else:
            logger.debug('Paralleltask - no workunits retrieved, idling')


    def get_work_unit(self):
        """
        Get the next work unit, by default a ParallelTask expects a list of values/tuples.
        When a arg is retrieved its removed from the list and placed in the in progress list.
        The arg is saved so that if the node fails the args can be re-run on another node

        This method *MUST* lock while it is altering the lists of data
        """
        logger.debug('Paralleltask - getting a workunit')
        data = None
        with self._lock:

            #grab from the beginning of the list
            if len(self._data) != 0:
                data = self._data.pop(0)
            else:
                return None, None

            self._workunit_count += 1

            #remove from _data and add to in_progress
            self._data_in_progress[self._workunit_count] = data
        logger.debug('Paralleltask - got a workunit: %s %s' % (data, self._workunit_count))

        return data, self._workunit_count;


    def _work_unit_complete(self, results, index, local=False):
        """
        A work unit completed.  Handle the common management tasks to remove the data
        from in_progress.  Also call task specific work_unit_complete(...)

        This method *MUST* lock while it is altering the lists of data
        """
        logger.debug('Paralleltask - Work unit completed, local=%s' % local)
        with self._lock:
            # run the task specific post process
            self.work_unit_complete(self._data_in_progress[index], results)

            # remove the workunit from _in_progress
            del self._data_in_progress[index]

            #check stop flag
            if self.STOP_FLAG:
                self.task_complete(None)

            #check for more work
            if not (len(self._data_in_progress) or len(self._data)):
                #all work is done, call the task specific function to combine the results 
                logger.debug('Paralleltask - all workunits complete, calling task post process')
                results = self.work_complete()
                self._complete(results)
                return

        # start another work unit.  its possible there is only 1 unit left and multiple
        # workers completing at the same time reaching this call.  _assign_work() 
        # will handle the locking.  It will cause some threads to fail to get work but
        # that is expected.  to lock here would require a reentrant lock which is slower
        logger.debug('Paralleltask - still has more work: %s :  %s' % (len(self._data), len(self._data_in_progress)))
        self._assign_work(local)


    def _worker_failed(self, index):
        """
        A worker failed while working.  re-add the data to the list
        """
        logger.warning('Paralleltask - Worker failure during workunit')
        with self._lock:

            #remove data from in progress
            data = self._data_in_progress[index]
            del self._data_in_progress[index]

            #add data to the end of the list
            self._data.append(data)
