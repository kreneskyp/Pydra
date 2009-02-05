from __future__ import with_statement
from threading import Thread, Lock
from abstract import *
from twisted.internet import threads
import time

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

    parent = None
    _status = STATUS_STOPPED
    __callback = None
    workunit = None
    msg = None
    description = 'Default description about Task baseclass.'

    def __init__(self, msg=None):
        self.msg = msg

        _work = AbstractMethod('_work')
        progress = AbstractMethod('progress')
        progressMessage = AbstractMethod('progressMessage')
        _reset = AbstractMethod('_reset')

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
    def start(self, args={}, subtask_key=None, callback=None, callback_args=None):
        # only start if not already running
        if self._status == STATUS_RUNNING:
            return

        #if this was subtask find it and execute just that subtask
        if subtask_key:
            print  ' starting subtask'
            split = subtask_key.split('.')
            subtask = self.get_subtask(split)
            print  '    - got subtask'
            deferred = threads.deferToThread(subtask.work, args)

        #else this is a normal task just execute it
        else:
            print ' starting normal task'
            print '[%s]' % args
            deferred = threads.deferToThread(self.work, args)

        if callback:
            if callback_args:
                deferred.addCallback(callback, *callback_args)
            else:
                deferred.addCallback(callback)

        return 1

    """
    Does the work of the task.  This is can be called directly for synchronous work or via start which
    causes a workunit thread to be spawned and call this function.  this method will set flags properly and
    delegate implementation specific work to _work(args)
    """
    def work(self, args):
        self._status = STATUS_RUNNING
        results = self._work(**args)
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
    Requests a worker for a subtask from the tasks parent.  calling this on any task will
    cause requests to bubble up to the root task whose parent will be the worker
    running the task.
    """
    def request_worker(self, task_key, args):
        self.parent.request_worker(self, task_key, args)

    """
    Retrieves the worker running this task.  This function is recursive and bubbles
    up through the task tree till the worker is reached
    """
    def get_worker(self):
        return self.parent.get_worker()


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
    def _generate_key(self):
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

    """
    Retrieves a subtask via task_path.  Task_path is the task_key
    split into a list for easier iteration
    """
    def get_subtask(self, task_path):
        #A Task can't have children,  if this is the last entry in the path
        # then this is the right tsk
        if len(task_path) == 1:
            return self
        else:
            raise Exception("Task not found")

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

        for task in self.subtasks:
            task.parent = self

    def addTask(self, task, percentage=None):
        subtask = SubTaskWrapper(task, percentage)
        self.subtasks.append(subtask)
        task.id = '%s-%d' % (self.id,len(self.subtasks))

    def reset(self):
        for subtask in self.subtasks:
            subtask.task.reset()

    def get_subtask(self, task_path):
        #pop this classes name off the list
        task_path.pop(0)

        #recurse down into the child
        return self.subtasks[task_path[0]].get_subtask(task_path)


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
    _available_workers = 1
    _data = None
    _data_in_progress = []
    _in_progress_count = 0
    subtask = None
    subtask_key = None

    def __init__(self, msg=None):
        Task.__init__(self, msg)
        self._lock = Lock()

    def __setattr__(self, key, value):
        Task.__setattr__(self, key, value)
        if key == 'subtask':
            value.parent = self


    """
    Work function overridden to delegate workunits to other Workers.
    """
    def _work(self, **kwargs):

        # save data, if any
        if kwargs and kwargs.has_key('data'):
            self._data = kwargs['data']

        self.subtask_key = self.subtask._generate_key()

        print 'getting count of available workers'
        self._available_workers = self.get_worker().available_workers
        print ' -starting ptask'

        # prepopulate the in progress array so data can be tracked easier
        self._data_in_progress = [None for i in range(self._available_workers)]

        # if theres more than one worker assign values
        # this check is required for cases where this is run
        # on a single core machine.  in that case this worker
        # is the only worker that exists
        if self._available_workers > 1:
            #assign initial set of work to other workers
            for i in range(self._available_workers):
                print '  trying to assign work'
                self._assign_work()

        #start a work_unit locally
        self._assign_work_local()

        print ' -initial work assigned'
        # loop until all the data is processed
        more_work = True
        while more_work:
            # sleep for a bit
            time.sleep(5)

            # check to see if there is either work in progress or work left to process
            with self._lock:
                more_work = self._in_progress_count or len(self._data)
                print 'more work!!!'

        #all work is done, call the task specific function to combine the results
        return self.work_complete()

    def get_subtask(self, task_path):
        #pop this class off the list
        task_path.pop(0)

        #recurse down into the child
        return self.subtask.get_subtask(task_path)


    """
    assign a unit of work to a Worker by requesting a worker from the compute cluster
    """
    def _assign_work(self):
        data, index = self.get_work_unit()
        if data:
            print '  -assigning work'
            self.parent.request_worker(self.subtask, {'data':data})

    """
    assign a unit of work to this Worker
    """
    def _assign_work_local(self):
        print '  -assigning work locally'
        data, index = self.get_work_unit()
        if not data == None:
            print '    -starting work locally'
            self.subtask.start({'data':data}, callback=self._local_work_unit_complete, callback_args=(index,))

    """
    Get the next work unit, by default a ParallelTask expects a list of values/tuples.
    When a arg is retrieved its removed from the list and placed in the in progress list.
    The arg is saved so that if the node fails the args can be re-run on another node

    This method *MUST* lock while it is altering the lists of data
    """
    def get_work_unit(self):
        print '  -getting a workunit'
        data = None
        with self._lock:
            #grab from the beginning of the list
            data = self._data.pop(0)

            #use index is specified, were
            index = self._in_progress_count

            #remove from _data and add to in_progress
            self._data_in_progress[index] = data
            self._in_progress_count = index+1

        return data, index;


    """
    A work unit completed.  Handle the common management tasks to remove the data
    from in_progress.  Also call task specific work_unit_complete(...)

    This method *MUST* lock while it is altering the lists of data
    """
    def _work_unit_complete(self, results, index):
        print 'Work unit completed'
        with self._lock:
            # run the task specific post process
            self.work_unit_complete(self._in_progress_count[index], results)

            # remove the workunit from _in_progress
            self._data_in_progress[index] = None
            self._in_progress_count -= 1

        # start another work unit
        if len(self._data):
            self._assign_work()


    """
    A work unit completed.  Handle the common management tasks to remove the data
    from in_progress.  Also call task specific work_unit_complete(...)

    This method *MUST* lock while it is altering the lists of data
    """
    def _local_work_unit_complete(self, results, index):
        print 'Work unit completed'
        with self._lock:
            # run the task specific post process
            self.work_unit_complete(self._data_in_progress[index], results)

            # remove the workunit from _in_progress
            self._data_in_progress[index] = None
            self._in_progress_count -= 1

        # start another work unit
        if len(self._data):
            self._assign_work_local()


    """
    A work unit failed.  re-add the data to the list
    """
    def _work_unit_failed(self, data, results):
        print 'Work unit failed'
        pass