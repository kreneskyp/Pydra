from __future__ import with_statement
from threading import Thread, Lock
from abstract import *

from twisted.internet import threads

STATUS_FAILED = -1;
STATUS_STOPPED = 0;
STATUS_RUNNING = 1;
STATUS_PAUSED = 2;
STATUS_COMPLETE = 3;

"""
SubTaskWrapper - class used to store additional information
about the relationship between a container and a subtask.

    percentage - the percentage of work the task accounts for.
"""
class SubTaskWrapper():
    def __init__(self, task, percentage):
        self.task = task
        self.percentage = percentage


"""
Task - class that wraps a set of functions as a runnable unit of work.  Once
wrapped the task allows functions to be managed and tracked in a uniform way.

This is an abstract class and requires the following functions to be implemented:
    * _work  -  does the work of the task
    * _reset - resets the state of the task when stopped
    * progress - returns the state of the task as an integer from 0 to 100
    * progressMessage - returns the state of the task as a readable string
"""
class Task(object):

    def __init__(self, msg=None):
        self.msg = msg

        _work = AbstractMethod('_work')
        progress = AbstractMethod('progress')
        progressMessage = AbstractMethod('progressMessage')
        _reset = AbstractMethod('_reset')

        self.__callback = None
        self.workunit = None
        self._status = STATUS_STOPPED
        self.id = 1

    """
    Resets the task, including setting the flags properly,  delegates implementation
    specific work to _reset()
    """
    def reset(self):
        self._status = STATUS_STOPPED
        self._reset()

    """
    starts the task.  This will spawn the work in a workunit thread.
    """
    def start(self, args=None, callback=None):
        # only start if not already running
        if self._status == STATUS_RUNNING:
            return

        deferred = threads.deferToThread(self.work, args)

        if callback:
            deferred.addCallback(callback)

        return 1

    """
    Does the work of the task.  This is can be called directly for synchronous work or via start which
    causes a workunit thread to be spawned and call this function.  this method will set flags properly and
    delegate implementation specific work to _work(args)
    """
    def work(self, args):
        self._status = STATUS_RUNNING
        results = self._work(args)
        self._status = STATUS_COMPLETE

        #if self.__callback:
        #    self.__callback(args, results)

        return results

    """
    Returns the status of this task.  Used as a function rather than member variable so this
    function can be overridden
    """
    def status(self):
        return self._status

    """
    Requests a worker from the tasks parent.  calling this on any task will
    cause requests to bubble up to the root task whose parent will be the worker
    running the task.
    """
    def request_worker(self, task_key, args):
        self.parent.request_worker(self, task_key, args)


    """
    Get the key that represents this task instance.  This key will give
    the path required to find it if iterating from the root of the task.
    This is used so that subtasks can be selected.
    """
    def get_key(self):
        return self._generate_key()

    """
    Generate the key for this task using a recursive algorithm.
    """
    def _generate_key():
        #check to see if this tasks parent is a ParallelTask
        # a ParallelTask can only have one child so the key is 
        # just the classname
        if issubclass(self.parent, (ParallelTask,)):
            #recurse to parent
            base = self.parent.get_key()
            #combine
            key = '%s.%s' % (base, self.__class__.__name__)


        #check to see if this tasks parent is a TaskContainer
        # TaskContainers can have multiple children who might
        # all have the same class.  The key must include
        elif issubclass(self.parent, (TaskContainer,)):
            #recurse to parent
            base = self.parent.get_key()
            #combine
            index = self.parent.subtasks.index(self)
            key = '%s.%s:%i' % (base, self.__class__.__name__, index)

        #The only other choice is that the parent is a Worker
        # in which case this is the root instance, just return
        # its class name
        else:
            key = self.__class__.__name__

        self.key = key
        return key

    """
    Retrieves a subtask via key.  Use the key to drilldown through
    The subtasks to find the requested subtask.
    """
    def get_subtask(self, task_key):
        pass

"""
TaskContainer - an extension of Task that contains other tasks

TaskContainer does no work itself.  Its purpose is to allow a bigger job
to be broken into discrete functions.  IE.  downloading and processing.
"""
class TaskContainer(Task):

    def __init__(self, msg, sequential=True):
        Task.__init__(self, msg)
        self.subtasks = []
        self.sequential = sequential

    def addTask(self, task, percentage=None):
        subtask = SubTaskWrapper(task, percentage)
        self.subtasks.append(subtask)
        task.id = '%s-%d' % (self.id,len(self.subtasks))

    def reset(self):
        for subtask in self.subtasks:
            subtask.task.reset()

    # Starts the task running all subtasks
    def _work(self, args=None):
        self.reset()

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


    """
    calculatePercentage - determines the percentage of work that each
    child task accounts for.

    TODO: take into account tasks that have had weighting manually set. 
    """
    def calculatePercentage(self):
        return float(1)/len(self.subtasks);


    """
    progress - returns the progress as a number 0-100.  

    A container task's progress is a derivitive of its children.
    the progress of the child counts for a certain percentage of the 
    progress of the parent.  This weighting can be set manually or
    divided evenly by calculatePercentage()
    """
    def progress(self):
        progress = 0
        for subtask in self.subtasks:
            if subtask.percentage == None:
                percentage = self.calculatePercentage()
            else:
                percentage = subtask.percentage

            # if task is done it complete 100% of its work 
            if subtask.task._status == STATUS_COMPLETE:
                progress += 100*percentage
            # task is only partially complete
            else:
                progress += subtask.task.progress()*percentage

        return progress

    """ 
    returns a plain text status message
    """
    def progressMessage(self):
        for subtask in self.subtasks:
            if subtask.task._status == STATUS_RUNNING:
                return subtask.task.progressMessage()

        return None

    """
    getStatus - returns status of this task.  A container task's status is 
    a derivitive of its children.

    failed - if any children failed, then the task failed
    running - if any children are running then the task is running
    paused - paused if no other children are running
    complete - complete if all children are complete
    stopped - default response if no other conditions are met
    """
    def status(self):
        has_paused = False;
        has_unfinished = False;

        for subtask in self.subtasks:
            subtaskStatus = subtask.task.status()
            if subtaskStatus == STATUS_RUNNING:
                # we can return right here because if any child is running the 
                # container is considered to be running
                return STATUS_RUNNING

            elif subtaskStatus == STATUS_FAILED:
                # we can return right here because if any child failed then the
                # container is considered to be failed.  All other running tasks
                # should be stopped on failure.
                return STATUS_FAILED

            elif subtaskStatus == STATUS_PAUSED:
                has_paused = True

            elif subtaskStatus <> STATUS_COMPLETE:
                has_unfinished = True

        # Task is not running or failed.  If any are paused
        # then the container is paused
        if has_paused:
            return STATUS_PAUSED

        # task is not running, failed, or paused.  if all children are complete then it is complete
        if not has_unfinished:
            return STATUS_COMPLETE

        # only status left it could be is STOPPED
        return STATUS_STOPPED


"""
ParallelTask - is a task that can be broken into discrete work units
"""
class ParallelTask(Task):
    _lock = None 
    _node_count = 1
    _paralell_task = None
    _data = None
    _data_in_progress = None
    _task = None


    def __init__(self, task, msg=None):
        Task.__init__(self, msg)
        self._node_count = 1
        self._lock = Lock()
        self._task=task


    """
    Work function overridden to delegate workunits to other Workers.
    """
    def _work(self, data):
        # save data
        self._data = data

        #assign initial set of work to nodes
        for i in range(self._node_count):
            self._assign_work()

        # loop until all the data is processed
        more_work = True
        while more_work:
            # sleep for a bit
            time.sleep(5)

            # check to see if there is either work in progress or work left to process
            with self._lock:
                more_work = len(self._data_in_progress) or len(self._data):



    """
    assign a unit of work to a node by requesting a worker
    from the compute cluster
    """
    def _assign_work(self):
        data = self.get_work_unit()
        if data:
            self.parent.request_worker(self, data)

    """
    Get the next work unit, by default a ParallelTask expects a list of values/tuples.
    When a arg is retrieved its removed from the list and placed in the in progress list.
    The arg is saved so that if the node fails the args can be re-run on another node

    This method *MUST* lock while it is altering the lists of data
    """
    def get_work_unit():
        data = None
        with self._lock:
            #grab from the beginning of the list
            data = self._data.pop(0)

            #remove from _data and add to in_progress
            self._data_in_progress.append(data)

        return data;

    """
    A work unit completed.  Handle the common management tasks to remove the data
    from in_progress.  Also call task specific work_unit_complete(...)

    This method *MUST* lock while it is altering the lists of data
    """
    def _work_unit_complete(data, results):
        print 'Work unit completed'
        with self._lock:
            # run the task specific post process
            self.work_unit_complete(data, results)

            # remove the workunit from _in_progress
            _data_in_progress.remove(data)

        # start another work unit
        if len(self._data):
            self._assign_work()


    """
    A work unit failed.  re-add the data to the list
    """
    def _work_unit_failed(data, results):
        print 'Work unit failed'
        pass