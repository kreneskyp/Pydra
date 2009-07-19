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

import settings
import datetime
from pydra_server.cluster.module import Module, REMOTE_WORKER, REMOTE_NODE
from pydra_server.cluster.tasks import STATUS_STOPPED, STATUS_RUNNING, STATUS_COMPLETE, STATUS_CANCELLED, STATUS_FAILED
#from pydra_server.cluster.tasks.task_manager import TaskManager
from pydra_server.models import TaskInstance
from pydra_server.util import deprecated

# init logging
import logging
logger = logging.getLogger('root')


class TaskScheduler(Module):

    """
    Handles Scheduling tasks

    Methods:

        == worker availability ==
        remove_worker - called on worker disconnect
        #return_work_success
        #return_work_failed
        worker_connected
        worker_status_returned
        
        == scheduling ==
        queue_task - add task to queue
        cancel_task
        advance_queue - pick next task
        run_task - sends task to worker
        run_task_successful
        select_worker

        == task reporting ==
        send_results
        task_failed
        worker_stopped
        request_worker

        == task status tracking ==
        fetch_task_status
        fetch_task_status_success
        task_statuses        
    """

    _signals = [
        'TASK_QUEUED',
        'TASK_STARTED',
        'TASK_FAILED',
        'TASK_FINISHED',
        'WORKUNIT_REQUESTED',
        'WORKUNIT_COMPLETED',
        'WORKUNIT_FAILED'
    ]
 
    _shared = [
        '_running_workers',
        'registry'
    ]    

    def __init__(self, manager):

        self._listeners = {
            'WORKER_DISCONNECTED':self.remove_worker,
            'WORKER_AUTHENTICATED': self.worker_connected,
            'CANCEL_TASK': self.cancel_task,
        }

        self._remotes = [
            (REMOTE_WORKER, self.request_worker),
            (REMOTE_WORKER, self.send_results),
            (REMOTE_WORKER, self.worker_stopped),
            (REMOTE_WORKER, self.task_failed)
        ]

        self._interfaces = [
            self.task_statuses,
            self.cancel_task,
            ('_queue','list_queue'),
            ('_running','list_running')
        ]


        Module.__init__(self, manager)

        # locks
        self._lock_queue = Lock()   #for access to _queue

        # load tasks queue
        self._running = list(TaskInstance.objects.running())
        self._running_workers = {}
        self._queue = list(TaskInstance.objects.queued())

        # task statuses
        self._task_statuses = {}
        self._next_task_status_update = datetime.datetime.now()

        # workers
        self._workers_idle = []
        self._workers_working = {}

        #load tasks that are cached locally
        #the master won't actually run the tasks unless there is also
        #a node running locally, but it will be used to inform the controller what is available
        #self.task_manager = TaskManager()
        #self.task_manager.autodiscover()
        #self.available_tasks = self.task_manager.registry


    def remove_worker(self, worker_key):
        """
        Called when a worker disconnects
        """
        with self._lock:
            # if idle, just remove it.  no need to do anything else
            if worker_key in self._workers_idle:
                logger.info('worker:%s - removing worker from idle pool' % worker_key)
                self._workers_idle.remove(worker_key)

            #worker was working on a task, need to clean it up
            else:
                removed_worker = self._workers_working[worker_key]

                #worker was working on a subtask, return unfinished work to main worker
                if removed_worker[3]:
                    logger.warning('%s failed during task, returning work unit' % worker_key)
                    task_instance = TaskInstance.objects.get(id=removed_worker[0])
                    main_worker = self.workers[task_instance.worker]
                    if main_worker:
                        d = main_worker.remote.callRemote('return_work', removed_worker[3], removed_worker[4])
                        d.addCallback(self.return_work_success, worker_key)
                        d.addErrback(self.return_work_failed, worker_key)

                    else:
                        #if we don't have a main worker listed it probably already was disconnected
                        #just call successful to clean up the worker
                        self.return_work_success(None, worker_key)

                #worker was main worker for a task.  cancel the task and tell any
                #workers working on subtasks to stop.  Cannot recover from the 
                #main worker going down
                else:
                    #TODO
                    pass

    def return_work_success(self, results, worker_key):
        """
        Work was sucessful returned to the main worker
        """
        with self._lock:
            del self._workers_working[worker_key]


    def return_work_failed(self, results, worker_key):
        """
        A worker disconnected and the method call to return the work failed
        """
        pass


    def select_worker(self, task_instance_id, task_key, args={}, subtask_key=None, workunit_key=None):
        """
        Select a worker to use for running a task or subtask
        """
        #lock, selecting workers must be threadsafe
        with self._lock:
            if len(self._workers_idle):
                #move the first worker to the working state storing the task its working on
                worker_key = self._workers_idle.pop(0)
                self._workers_working[worker_key] = (task_instance_id, task_key, args, subtask_key, workunit_key)

                #return the worker object, not the key
                return self.workers[worker_key]
            else:
                return None


    def queue_task(self, task_key, args={}, subtask_key=None):
        """
        Queue a task to be run.  All task requests come through this method.  It saves their
        information in the database.  If the cluster has idle resources it will start the task
        immediately, otherwise it will queue the task until it is ready.
        """
        logger.info('Task:%s:%s - Queued:  %s' % (task_key, subtask_key, args))

        #create a TaskInstance instance and save it
        task_instance = TaskInstance()
        task_instance.task_key = task_key
        task_instance.subtask_key = subtask_key
        task_instance.args = simplejson.dumps(args)
        task_instance.save()

        #queue the task and signal attempt to start it
        with self._lock_queue:
            self._queue.append(task_instance)
        self.advance_queue()

        return task_instance

    @deprecated('Functionality will be moved to queue_task')
    def interface_run_task(self, _, task_key, args=None):
        """
        Runs a task.  It it first placed in the queue and the queue manager
        will run it when appropriate.

        Args should be a dictionary of values.  It is acceptable for this to be
        improperly typed data.  ie. Integer given as a String.  This function
        will parse and clean the args using the form class for the Task
        """

        # args coming from the controller need to be parsed by the form. This
        # will give proper typing to the data and allow validation.
        if args:
            task = self.master.available_tasks[task_key]
            form_instance = task.form(args)
            if form_instance.is_valid():
                # repackage properly cleaned data
                args = {}
                for key, val in form_instance.cleaned_data.items():
                    args[key] = val

            else:
                # not valid, report errors.
                return {
                    'task_key':task_key,
                    'errors':form_instance.errors
                }

        task_instance =  self.master.queue_task(task_key, args=args)

        return {
                'task_key':task_key,
                'instance_id':task_instance.id,
                'time':time.mktime(task_instance.queued.timetuple())
               }



    def cancel_task(self, task_id):
        """
        Cancel a task.  This function is used to cancel a task that was scheduled. 
        If the task is in the queue still, remove it.  If it is running then
        send signals to all workers assigned to it to stop work immediately.
        """
        task_instance = TaskInstance.objects.get(id=task_id)
        logger.info('Cancelling Task: %s' % task_id)
        with self._lock_queue:
            if task_instance in self._queue:
                #was still in queue
                self._queue.remove(task_instance)
                logger.debug('Cancelling Task, was in queue: %s' % task_id)
            else:
                logger.debug('Cancelling Task, is running: %s' % task_id)
                #get all the workers to stop
                for worker_key, worker_task in self._workers_working.items():
                    if worker_task[0] == task_id:
                        worker = self.workers[worker_key]
                        logger.debug('signalling worker to stop: %s' % worker_key)
                        worker.remote.callRemote('stop_task')

                self._running.remove(task_instance)

            task_instance.completion_type = STATUS_CANCELLED
            task_instance.save()

            return 1


    def advance_queue(self):
        """
        Advances the queue.  If there is a task waiting it will be started, otherwise the cluster will idle.
        This should be called whenever a resource becomes available or a new task is queued
        """
        logger.debug('advancing queue: %s' % self._queue)
        with self._lock_queue:
            try:
                task_instance = self._queue[0]

            except IndexError:
                #if there was nothing in the queue then fail silently
                logger.debug('No tasks in queue, idling')
                return False

            if self.run_task(task_instance.id, task_instance.task_key, simplejson.loads(task_instance.args), task_instance.subtask_key):
                #task started, update its info and remove it from the queue
                logger.info('Task:%s:%s - starting' % (task_instance.task_key, task_instance.subtask_key))
                task_instance.started = datetime.datetime.now()
                task_instance.completion_type = STATUS_RUNNING
                task_instance.save()

                del self._queue[0]
                self._running.append(task_instance)

            else:
                # cluster does not have idle resources.
                # task will stay in the queue
                logger.debug('Task:%s:%s - no resources available, remaining in queue' % (task_instance.task_key, task_instance.subtask_key))
                return False


    def run_task(self, task_instance_id, task_key, args={}, subtask_key=None, workunit_key=None):
        """
        Run the task specified by the task_key.  This shouldn't be called directly.  Tasks should
        be queued with queue_task().  If the cluster has idle resources it will be run automatically

        This function is used internally by the cluster for parallel processing work requests.  Work
        requests are never queued.  If there is no resource available the main worker for the task
        should be informed and it can readjust its count of available resources.  Any type of resource
        sharing logic should be handled within select_worker() to keep the logic organized.
        """

        # get a worker for this task
        worker = self.select_worker(task_instance_id, task_key, args, subtask_key, workunit_key)
        # determine how many workers are available for this task
        available_workers = len(self._workers_idle)+1

        if worker:
            logger.debug('Worker:%s - Assigned to task: %s:%s %s' % (worker.name, task_key, subtask_key, args))
            d = worker.remote.callRemote('run_task', task_key, args, subtask_key, workunit_key, available_workers)
            d.addCallback(self.run_task_successful, worker, task_instance_id, subtask_key)
            return worker

        # no worker was available
        # just return 0 (false), the calling function will decided what to do,
        # depending what called run_task, different error handling will apply
        else:
            logger.warning('No worker available')
            return None

    def run_task_successful(self, results, worker, task_instance_id, subtask_key=None):

        #save the history of what workers work on what task/subtask
        #its needed for tracking finished work in ParallelTasks and will aide in Fault recovery
        #it might also be useful for analysis purposes if one node is faulty
        if subtask_key:
            #TODO, update model and record what workers worked on what subtasks
            pass

        else:
            task_instance = TaskInstance.objects.get(id=task_instance_id)
            task_instance.worker = worker.name
            task_instance.save()


    def send_results(self, worker_key, results, workunit_key):
        """
        Called by workers when they have completed their task.

            Tasks runtime and log should be saved in the database
        """
        logger.debug('Worker:%s - sent results: %s' % (worker_key, results))
        with self._lock:
            task_instance_id, task_key, args, subtask_key, workunit_key = self._workers_working[worker_key]
            logger.info('Worker:%s - completed: %s:%s (%s)' % (worker_key, task_key, subtask_key, workunit_key))

            # release the worker back into the idle pool
            # this must be done before informing the 
            # main worker.  otherwise a new work request
            # can be made before the worker is released
            del self._workers_working[worker_key]
            self._workers_idle.append(worker_key)

            #if this was the root task for the job then save info.  Ignore the fact that the task might have
            #been canceled.  If its 100% complete, then mark it as such.
            if not subtask_key:
                with self._lock_queue:
                    task_instance = TaskInstance.objects.get(id=task_instance_id)
                    task_instance.completed = datetime.datetime.now()
                    task_instance.completion_type = STATUS_COMPLETE
                    task_instance.save()

                    #remove task instance from running queue
                    try:
                        self._running.remove(task_instance)
                    except ValueError:
                        # was already removed by cancel
                        pass


            else:
                #check to make sure the task was still in the queue.  Its possible this call was made at the same
                # time a task was being canceled.  Only worry about sending the reults back to the Task Head
                # if the task is still running
                task_instance = TaskInstance.objects.get(id=task_instance_id)
                with self._lock_queue:
                    if task_instance in self._running:
                        #if this was a subtask the main task needs the results and to be informed
                        task_instance = TaskInstance.objects.get(id=task_instance_id)
                        main_worker = self.workers[task_instance.worker]
                        logger.debug('Worker:%s - informed that subtask completed' % 'FOO')
                        main_worker.remote.callRemote('receive_results', results, subtask_key, workunit_key)
                    else:
                        logger.debug('Worker:%s - returned a subtask but the task is no longer running.  discarding value.' % worker_key)


        #attempt to advance the queue
        self.advance_queue()


    def task_failed(self, worker_key, results, workunit_key):
        """
        Called by workers when the task they were running throws an exception
        """
        with self._lock:
            task_instance_id, task_key, args, subtask_key, workunit_key = self._workers_working[worker_key]
            logger.info('Worker:%s - failed: %s:%s (%s)' % (worker_key, task_key, subtask_key, workunit_key))


            # cancel the task and send notice to all other workers to stop
            # working on this task.  This may be partially recoverable but that
            # is not included for now.
            with self._lock_queue:

                # release the worker back into the idle pool
                del self._workers_working[worker_key]
                self._workers_idle.append(worker_key)

                task_instance = TaskInstance.objects.get(id=task_instance_id)
                task_instance.completed = datetime.datetime.now()
                task_instance.completion_type = STATUS_FAILED
                task_instance.save()

                for worker_key, worker_task in self._workers_working.items():
                    if worker_task[0] == task_instance_id:
                        worker = self.workers[worker_key]
                        logger.debug('signalling worker to stop: %s' % worker_key)
                        worker.remote.callRemote('stop_task')

                #remove task instance from running queue
                try:
                    self._running.remove(task_instance)
                except ValueError:
                    # was already removed
                    pass

        #attempt to advance the queue
        self.advance_queue()

    def worker_stopped(self, worker_key):
        """
        Called by workers when they have stopped due to a cancel task request.
        """
        with self._lock:
            logger.info(' Worker:%s - stopped' % worker_key)

            # release the worker back into the idle pool
            # this must be done before informing the 
            # main worker.  otherwise a new work request
            # can be made before the worker is released
            del self._workers_working[worker_key]
            self._workers_idle.append(worker_key)

        #attempt to advance the queue
        self.advance_queue()


    def request_worker(self, workerAvatar, subtask_key, args, workunit_key):
        """
        Called by workers running a Parallel task.  This is a request
        for a worker in the cluster to process a workunit from a task
        """

        #get the task key and run the task.  The key is looked up
        #here so that a worker can only request a worker for the 
        #their current task.
        worker = self._workers_working[workerAvatar.name]
        task_instance = TaskInstance.objects.get(id=worker[0])
        logger.debug('Worker:%s - request for worker: %s:%s' % (workerAvatar.name, subtask_key, args))

        # lock queue and check status of task to ensure no lost workers
        # due to a canceled task
        with self._lock_queue:
            if task_instance in self._running:
                self.run_task(worker[0], worker[1], args, subtask_key, workunit_key)

            else:
                logger.debug('Worker:%s - request for worker failed, task is not running' % (workerAvatar.name))


    def worker_connected(self, worker_avatar):
        """
        Callback when a worker has been successfully authenticated
        """
        #request status to determine what this worker was doing
        deferred = worker_avatar.remote.callRemote('status')
        deferred.addCallback(self.worker_status_returned, worker=worker_avatar, worker_key=worker_avatar.name)


    def worker_status_returned(self, result, worker, worker_key):
        """
        Add a worker avatar as worker available to the cluster.  There are two possible scenarios:
        1) Only the worker was started/restarted, it is idle
        2) Only master was restarted.  Workers previous status must be reestablished

        The best way to determine the state of the worker is to ask it.  It will return its status
        plus any relevent information for reestablishing it's status
        """
        # worker is working and it was the master for its task
        if result[0] == WORKER_STATUS_WORKING:
            logger.info('worker:%s - is still working' % worker_key)
            #record what the worker is working on
            #self._workers_working[worker_key] = task_key

        # worker is finished with a task
        elif result[0] == WORKER_STATUS_FINISHED:
            logger.info('worker:%s - was finished, requesting results' % worker_key)
            #record what the worker is working on
            #self._workers_working[worker_key] = task_key

            #check if the Worker acting as master for this task is ready
            if (True):
                #TODO
                pass

            #else not ready to send the results
            else:
                #TODO
                pass

        #otherwise its idle
        else:
            with self._lock:
                self.workers[worker_key] = worker
                # worker shouldn't already be in the idle queue but check anyway
                if not worker_key in self._workers_idle:
                    self._workers_idle.append(worker_key)
                    logger.info('worker:%s - added to idle workers' % worker_key)



    def fetch_task_status(self):
        """
        updates the list of statuses.  this function is used because all
        workers must be queried to receive status updates.  This results in a
        list of deferred objects.  There is no way to block until the results
        are ready.  instead this function updates all the statuses.  Subsequent
        calls for status will be able to fetch the status.  It may be delayed 
        by a few seconds but thats minor when considering a task that could run
        for hours.

        For now, statuses are only queried for Main Workers.  Including 
        statuses of subtasks requires additional logic and overhead to pass the
        intermediate results to the main worker.
        """

        # limit updates so multiple controllers won't cause excessive updates
        now = datetime.datetime.now()
        if self._next_task_status_update < now:
            workers = self.workers
            for key, data in self._workers_working.items():
                if not data[3]:
                    worker = workers[key]
                    task_instance_id = data[0]
                    deferred = worker.remote.callRemote('task_status')
                    deferred.addCallback(self.fetch_task_status_success, task_instance_id)
            self.next_task_status_update = now + datetime.timedelta(0, 3)


    def fetch_task_status_success(self, result, task_instance_id):
        """
        updates task status list with response from worker used in conjunction
        with fetch_task_status()
        """
        self._task_statuses[task_instance_id] = result


    def task_statuses(self):
        """
        Returns the status of all running tasks.  This is a detailed list
        of progress and status messages.
        """

        # tell the master to fetch the statuses for the task.
        # this may or may not complete by the time we process the list
        self.fetch_task_status()

        statuses = {}
        for instance in self._queue:
            statuses[instance.id] = {'s':STATUS_STOPPED}

        for instance in self._running:
            start = time.mktime(instance.started.timetuple())

            # call worker to get status update
            try:
                progress = self._task_statuses[instance.id]

            except KeyError:
                # its possible that the progress does not exist yet. because
                # the task has just started and fetch_task_status is not complete
                pass
                progress = -1

            statuses[instance.id] = {'s':STATUS_RUNNING, 't':start, 'p':progress}

        return statuses
