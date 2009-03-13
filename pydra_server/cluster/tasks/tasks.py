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
import time

STATUS_FAILED = -1;
STATUS_STOPPED = 0;
STATUS_RUNNING = 1;
STATUS_PAUSED = 2;
STATUS_COMPLETE = 3;

class TaskNotFoundException(Exception):
    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)

class SubTaskWrapper():
    """
    SubTaskWrapper - class used to store additional information
    about the relationship between a container and a subtask.

    This class acts as a proxy for all Task methods

        percentage - the percentage of work the task accounts for.
    """
    def __init__(self, task, percentage):
        self.task = task
        self.percentage = percentage

    def get_subtask(self, task_path):
        return self.task.get_subtask(task_path)

    def __repr__(self):
        return self.task.__repr__()


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

    msg = None
    description = 'Default description about Task baseclass.'

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

    def start(self, args={}, subtask_key=None, callback=None, callback_args={}):
        """
        starts the task.  This will spawn the work in a workunit thread.
        """

        # only start if not already running
        if self._status == STATUS_RUNNING:
            return

        #if this was subtask find it and execute just that subtask
        if subtask_key:
            print  '[debug] Task - starting subtask %s - %s' % (subtask_key, args)
            split = subtask_key.split('.')
            subtask = self.get_subtask(split)
            print  '[debug] Task - got subtask'
            self.work_deferred = threads.deferToThread(subtask.work, args, callback, callback_args)

        #else this is a normal task just execute it
        else:
            print '[debug] Task - starting task: %s' % args
            self.work_deferred = threads.deferToThread(self.work, args, callback, callback_args)

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
        print '[debug] %s - Task - in Task.work()'  % self.get_worker().worker_key
        self._status = STATUS_RUNNING
        results = self._work(args)
        self._status = STATUS_COMPLETE
        print '[debug] %s - Task - work complete' % self.get_worker().worker_key

        self.work_deferred = None

        #make a callback, if any
        if callback:
            print '[debug] %s - Task - Making callback' % self
            callback(results, **callback_args)
        else:
            print '[warn] %s - Task - NO CALLBACK TO MAKE: %s' % (self, self.__callback)

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


    def _generate_key(self):
        """
        Generate the key for this task using a recursive algorithm.
        """
        #check to see if this tasks parent is a ParallelTask
        # a ParallelTask can only have one child so the key is 
        # just the classname
        if issubclass(self.parent.__class__, (ParallelTask,)):
            #recurse to parent
            base = self.parent.get_key()
            #combine
            key = '%s.%s' % (base, self.__class__.__name__)


        #check to see if this tasks parent is a TaskContainer
        # TaskContainers can have multiple children who might
        # all have the same class.  Use the index of the task
        elif issubclass(self.parent.__class__, (TaskContainer,)):
            #recurse to parent
            base = self.parent.get_key()
            #combine
            index = self.parent.subtasks.index(self)
            key = '%s.%i' % (base, index)

        #The only other choice is that the parent is a Worker
        # in which case this is the root instance, just return
        # its class name
        else:
            key = self.__class__.__name__

        self.key = key
        return key


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

class TaskContainer(Task):
    """
    TaskContainer - an extension of Task that contains other tasks

    TaskContainer does no work itself.  Its purpose is to allow a bigger job
    to be broken into discrete functions.  IE.  downloading and processing.
    """
    def __init__(self, msg, sequential=True):
        Task.__init__(self, msg)
        self.subtasks = []
        self.sequential = sequential

        for task in self.subtasks:
            task.parent = self

    def add_task(self, task, percentage=None):
        """
        Adds a task to the container
        """
        subtask = SubTaskWrapper(task, percentage)
        self.subtasks.append(subtask)
        task.parent=self
        task.id = '%s-%d' % (self.id,len(self.subtasks))

    def reset(self):
        for subtask in self.subtasks:
            subtask.task.reset()

    def get_subtask(self, task_path):
        """
        Overridden to deal with the oddity of how ContainerTask children are indicated
        in keys.  Children are indicated as integer indexes because there may be
        more than one of the same class.  Task.get_subtask(...) will break if the first
        element in the task_path is an integer.  If the task_path indicates the child
        of a ContainerTask the index will be replaced with the actual class before
        being passed on to the child
        """
        if len(task_path) == 1:
            if task_path[0] == self.__class__.__name__:
                return self
            else:
                raise TaskNotFoundException("Task not found")

        # pop this classes name off the list
        task_path.pop(0)

        # get index then swap index and class name
        try:
            index = int(task_path[0])
            child_class = self.subtasks[index].task.__class__.__name__
        except (ValueError, IndexError):
            raise TaskNotFoundException("Task not found")
        task_path[0] = child_class

        # recurse down into the child
        return self.subtasks[index].get_subtask(task_path)


    def _work(self, args=None):
        # Starts the task running all subtasks
        result = args
        for subtask in self.subtasks:
            print '   Starting Subtask: %s' % subtask
            if self.sequential:
                #sequential task, run the task work directly (default)
                result = subtask.task.work(result)
            else:
                #parallel task, run the subtask in its own thread
                result = subtask.task.start(result)

        return result


    def _stop(self):
        """
        Overridden to call stop on all children
        """
        Task._stop(self)
        for subtask in self.subtasks:
            subtask._stop()



    def progress(self):
        """
        progress - returns the progress as a number 0-100.  

        A container task's progress is a derivitive of its children.
        the progress of the child counts for a certain percentage of the 
        progress of the parent.  This weighting can be set manually or
        divided evenly by calculatePercentage()
        """
        progress = 0
        auto_total = 100
        auto_subtask_count = len(self.subtasks)
        for subtask in self.subtasks:
            if subtask.percentage:
                auto_total -= subtask.percentage
                auto_subtask_count -= 1
        auto_percentage = auto_total / auto_subtask_count / float(100)

        for subtask in self.subtasks:
            if subtask.percentage:
                percentage = subtask.percentage/float(100)
            else:
                percentage = auto_percentage

            # if task is done it complete 100% of its work 
            if subtask.task._status == STATUS_COMPLETE:
                progress += 100*percentage

            # task is only partially complete
            else:
                progress += subtask.task.progress()*percentage

        return progress


    def progressMessage(self):
        """ 
        returns a plain text status message
        """
        for subtask in self.subtasks:
            if subtask.task._status == STATUS_RUNNING:
                return subtask.task.progressMessage()

        return None


    def status(self):
        """
        getStatus - returns status of this task.  A container task's status is 
        a derivitive of its children.

        failed - if any children failed, then the task failed
        running - if any children are running then the task is running
        paused - paused if no other children are running
        complete - complete if all children are complete
        stopped - default response if no other conditions are met
        """
        has_paused = False
        has_unfinished = False
        has_failed = False

        for subtask in self.subtasks:
            status = subtask.task.status()
            if status == STATUS_RUNNING:
                # we can return right here because if any child is running the 
                # container is considered to be running.  This overrides STATUS_FAILED
                # because we want to indicate that the task is still doing something
                # even though it should be stopped
                return STATUS_RUNNING

            elif status == STATUS_FAILED:
                # mark has_failed flag.  this can still be overridden by a running task
                has_failed = True
                has_unfinished = True

            elif status == STATUS_PAUSED:
                # mark has_paused flag, can be overriden by failed or running tasks
                has_paused = True;
                has_unfinished = True

            elif status == STATUS_STOPPED:
                #still need to mark this status to indicate we arent complete
                has_unfinished = True

        if has_failed:
            return STATUS_FAILED

        if has_paused:
            return STATUS_PAUSED

        if not has_unfinished:
            return STATUS_COMPLETE

        # its not any other status, it must be stopped
        return STATUS_STOPPED



class ParallelTask(Task):
    """
    ParallelTask - is a task that can be broken into discrete work units
    """
    _lock = None                # general lock
    _available_workers = 1      # number of workers available to this task
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



    def work(self, args, callback, callback_args={}):
        """
        overridden to prevent early task cleanup.  ParallelTasl._work() returns immediately even though 
        work is likely running in the background.  There appears to be no effective way to block without 
        interupting twisted.Reactor.  The cleanup that normally happens in work() has been moved to
        task_complete() which will be called when there is no more work remaining.
        """
        self.__callback = callback
        self._callback_args=callback_args

        self._status = STATUS_RUNNING
        results = self._work(**args)

        return results


    def _stop(self):
        """
        Overridden to call stop on all children
        """
        Task._stop(self)
        self.subtask._stop()


    def task_complete(self, results):
        """
        Should be called when all workunits have completed
        """
        self._status = STATUS_COMPLETE

        #make a callback, if any
        if self.__callback:
            self.__callback(results)


    def _work(self, **kwargs):
        """
        Work function overridden to delegate workunits to other Workers.
        """

        # save data, if any
        if kwargs and kwargs.has_key('data'):
            self._data = kwargs['data']
            print '[debug] Paralleltask - data was passed in!!!!'

        self.subtask_key = self.subtask._generate_key()

        print '[debug] Paralleltask - getting count of available workers'
        self._available_workers = self.get_worker().available_workers
        print '[debug] Paralleltask - starting, workers available: %i' % self._available_workers


        # if theres more than one worker assign values
        # this check is required for cases where this is run
        # on a single core machine.  in that case this worker
        # is the only worker that exists
        if self._available_workers > 1:
            #assign initial set of work to other workers
            for i in range(1, self._available_workers):
                print '[debug] Paralleltask - trying to assign worker %i' % i
                self._assign_work()

        #start a work_unit locally
        #reactor.callLater(1, self._assign_work_local)
        self._assign_work_local()

        print '[debug] Paralleltask - initial work assigned'
        # loop until all the data is processed
        reactor.callLater(5, self.more_work)


    def more_work(self):
        # check to see if there is either work in progress or work left to process
            with self._lock:
                if self.STOP_FLAG:
                    self.task_complete(None)

                else:
                    #check for more work
                    if len(self._data_in_progress) or len(self._data):
                        print '[debug] Paralleltask - still has more work: %s :  %s' % (self._data, self._data_in_progress)
                        reactor.callLater(5, self.more_work)

                    #all work is done, call the task specific function to combine the results 
                    else:
                        results = self.work_complete()
                        self.task_complete(results)


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


    def _assign_work(self):
        """
        assign a unit of work to a Worker by requesting a worker from the compute cluster
        """
        data, index = self.get_work_unit()
        if not data == None:
            print '[debug] Paralleltask - assigning remote work'
            self.parent.request_worker(self.subtask.get_key(), {'data':data}, index)


    def _assign_work_local(self):
        """
        assign a unit of work to this Worker
        """
        print '[debug] Paralleltask - assigning work locally'
        data, index = self.get_work_unit()
        if not data == None:
            print '[debug] Paralleltask - starting work locally'
            self.subtask.start({'data':data}, callback=self._local_work_unit_complete, callback_args={'index':index})
        else:
            print '[debug] Paralleltask - no worker retrieved, idling'


    def get_work_unit(self):
        """
        Get the next work unit, by default a ParallelTask expects a list of values/tuples.
        When a arg is retrieved its removed from the list and placed in the in progress list.
        The arg is saved so that if the node fails the args can be re-run on another node

        This method *MUST* lock while it is altering the lists of data
        """
        print '[debug] Paralleltask - getting a workunit'
        data = None
        with self._lock:

            #grab from the beginning of the list
            data = self._data.pop(0)
            self._workunit_count += 1

            #remove from _data and add to in_progress
            self._data_in_progress[self._workunit_count] = data
        print '[debug] Paralleltask - got a workunit: %s %s' % (data, self._workunit_count)

        return data, self._workunit_count;


    def _work_unit_complete(self, results, index):
        """
        A work unit completed.  Handle the common management tasks to remove the data
        from in_progress.  Also call task specific work_unit_complete(...)

        This method *MUST* lock while it is altering the lists of data
        """
        print '[debug] Paralleltask - REMOTE Work unit completed'
        with self._lock:
            # run the task specific post process
            self.work_unit_complete(self._data_in_progress[index], results)

            # remove the workunit from _in_progress
            del self._data_in_progress[index]

        # start another work unit.  its possible there is only 1 unit left and multiple
        # workers completing at the same time reaching this call.  _assign_work() 
        # will handle the locking.  It will cause some threads to fail to get work but
        # that is expected.
        if len(self._data):
            self._assign_work()


    def _local_work_unit_complete(self, results, index):
        """
        A work unit completed.  Handle the common management tasks to remove the data
        from in_progress.  Also call task specific work_unit_complete(...)

        This method *MUST* lock while it is altering the lists of data
        """
        print '[debug] Paralleltask - LOCAL work unit completed'
        with self._lock:
            # run the task specific post process
            self.work_unit_complete(self._data_in_progress[index], results)

            # remove the workunit from _in_progress
            del self._data_in_progress[index]

        # start another work unit
        if len(self._data):
            self._assign_work_local()


    def _work_unit_failed(self, index):
        """
        A work unit failed.  re-add the data to the list
        """
        print '[warning] Paralleltask - Worker failure during workunit'
        with self._lock:

            #remove data from in progress
            data = self._data_in_progress[index]
            del self._data_in_progress[index]

            #add data to the end of the list
            self._data.append(data)