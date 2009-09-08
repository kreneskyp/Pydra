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


import time
from datetime import datetime, timedelta
import simplejson
from heapq import heappush, heappop, heapify
from twisted.internet import reactor, threads

from pydra.cluster.module import Module
from pydra.cluster.tasks import *
from pydra.cluster.tasks.task_manager import TaskManager
from pydra.cluster.constants import *
from pydra.models import TaskInstance, WorkUnit

# init logging
import logging
logger = logging.getLogger('root')


class WorkerJob:
    """
    Encapsulates a job that runs on a worker.
    """

    def __init__(self, root_task_id, task_key, args, subtask_key=None,
            workunit_key=None, on_main_worker=False):
        self.root_task_id = root_task_id
        self.task_key = task_key
        self.args = args
        self.subtask_key = subtask_key
        self.workunit_key = workunit_key
        self.on_main_worker = on_main_worker

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
        request_worker

        == Task Communication ==
        send_results
        task_failed
        worker_stopped


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
        'workers',
        '_idle_workers',
        '_active_workers',
    ]    

    def __init__(self, manager):

        self._listeners = {
            'WORKER_DISCONNECTED':self.remove_worker,
            'WORKER_CONNECTED':self.worker_connected,
            'CANCEL_TASK': self.cancel_task,
        }

        self._remotes = [
            ('NODE', self.request_worker),
            ('NODE', self.send_results),
            ('NODE', self.worker_stopped),
            ('NODE', self.task_failed),
            ('NODE', self.request_worker_release)
        ]

        self._friends = {
            'task_manager' : TaskManager,
        }

        self._interfaces = [
            self.task_statuses,
            self.cancel_task,
            self.queue_task,
            (self.get_queued_tasks, {'name':'list_queue'}),
            (self.get_running_tasks, {'name':'list_running'})
        ]


        Module.__init__(self, manager)

        # locks
        self._lock = Lock()         # general lock        
        self._worker_lock = Lock()  # lock for worker only transactions
        self._queue_lock = Lock()   # lock for queue only transactions

        # load tasks queue
        #self._running = list(TaskInstance.objects.running())
        #self._running_workers = {}
        #self._queue = list(TaskInstance.objects.queued())

        # task statuses
        self._task_statuses = {}
        self._next_task_status_update = datetime.now()

        # workers
        # self._workers_idle = []
        # self._workers_working = {}


        self._long_term_queue = []
        self._short_term_queue = []
        self._active_tasks = {}     # caching uncompleted task instances
        self._idle_workers = []     # all workers are seen equal
        self._active_workers = []
        self._worker_mappings = {}  # worker-job mappings
        self._waiting_workers = {}  # task-worker mappings

        # a set containing all main workers
        self._main_workers = set()

        self.update_interval = 5 # seconds
        #self._listeners = []

        #if listener:
        #    self.attach_listener(listener)

        self._init_queue()

        reactor.callLater(self.update_interval, self._update_queue)


    def _queue_task(self, task_key, args={}, priority=5):
        """
        Adds a (root) task that is to be run.

        Under the hood, the scheduler creates a task instance for the task, puts
        it into the long-term queue, and then tries to advance the queue.
        """
        logger.info('Task:%s - Queued:  %s' % (task_key, args))

        task_instance = TaskInstance()
        task_instance.task_key = task_key
        task_instance.args = simplejson.dumps(args)
        task_instance.priority = priority
        task_instance.subtask_key = None
        task_instance.queued = datetime.now()
        task_instance.status = STATUS_STOPPED
        task_instance.save()

        task_id = task_instance.id

        with self._queue_lock:
            heappush(self._long_term_queue, [task_instance.compute_score(), task_id])

            # cache this task
            self._active_tasks[task_id] = task_instance

        threads.deferToThread(self._schedule)

        return task_instance


    def cancel_task(self, task_id):
        """
        Cancel a task. Used to cancel a task that was scheduled.
        If the task is in the queue still, remove it.  If it is running then
        send signals to all workers assigned to it to stop work immediately.
        """
        task_id = int(task_id)
        with self._queue_lock:
            found = False
            # find the task in the ltq
            for i in range(len(self._long_term_queue)):
                if self._long_term_queue[i][1] == task_id:
                    logger.debug('Cancelling Task in LTQ: %s' % task_id)
                    del self._long_term_queue[i]
                    found = True
                    break
            else:
                for i in range(len(self._short_term_queue)):
                    if self._short_term_queue[i][1] == task_id:
                        logger.debug('Cancelling Task in STQ: %s' % task_id)
                        del self._short_term_queue[i]
                        found = True
                        for worker_key in self.get_workers_on_task(task_id):
                            worker = self.workers[worker_key]
                            logger.debug('Signalling Stop: %s' % worker_key)
                            worker.remote.callRemote('stop_task')
                        break
        return 1


    def add_worker(self, worker_key, task_status=None):
        """
        Adds a worker to the **idle pool**.

        Two possible invocation situations: 1) a new worker joins; and 2) a
        worker previously working on a work unit is returned to the pool.
        The latter case can be further categorized into several sub-cases, e.g.,
        task failure, task cancellation, etc. These sub-cases are identified by
        the third parameter, which is the final status of the task running on
        that worker.
        """
        with self._worker_lock:
            if worker_key in self._idle_workers:
                logger.warn('Worker is already in the idle pool: %s' %
                        worker_key)
                return

        job = self._worker_mappings.get(worker_key, None)
        if job:
            task_instance = self._active_tasks[job.root_task_id]
            if worker_key in self._main_workers:
                # this is a main worker
                if job.subtask_key:
                    # this main worker was working on a workunit
                    logger.info('Main worker:%s ready for work again' % worker_key)
                    work_unit = job.model
                    work_unit.completed = datetime.now()
                    work_unit.status = task_status
                    work_unit.save()
                    job.subtask_key = None
                    job.workunit_key = None

                else:
                    logger.info('Main worker:%s finishes the root task' %
                            worker_key)
                    with self._worker_lock:
                        self._main_workers.remove(worker_key)
                        self._idle_workers.append(worker_key)
                        del self._worker_mappings[worker_key]
                    status = STATUS_COMPLETE if task_status is None else task_status
                    task_instance.status = status
                    task_instance.completed = datetime.now()
                    task_instance.save()

                    with self._queue_lock:
                        if status in (STATUS_CANCELLED, STATUS_COMPLETE, STATUS_FAILED):
                            # safe to remove the task
                            length = len(self._short_term_queue)
                            for i in range(0, length):
                                # release any unreleased workers
                                for key in task_instance.waiting_workers:
                                    avatar = self.workers[key]
                                    avatar.remote.callRemote('release_worker')

                                if self._short_term_queue[i][1] == job.root_task_id:
                                    del self._short_term_queue[i]
                                    logger.info(
                                            'Task %d: %s is removed from the short-term queue' % \
                                            (job.root_task_id, job.task_key))
                                    break
                            heapify(self._short_term_queue)
            else:
                # not a main worker
                logger.info("Task %d returns a worker: %s" % (job.root_task_id,
                            worker_key))
                if job.subtask_key is not None:
                    # just double-check to make sure
                    with self._worker_lock:
                        del self._worker_mappings[worker_key]
                        task_instance.running_workers.remove(worker_key) 
                        self._idle_workers.append(worker_key)
        else:
            # a new worker
            logger.info('A new worker:%s is added' % worker_key)
            with self._worker_lock:
                self._idle_workers.append(worker_key)

        self._schedule()
 

    def remove_worker(self, worker_key):
        """
        Removes a worker from the idle pool.

        Returns True if this operation succeeds and False otherwise.
        """
        with self._worker_lock:
            job = self.get_worker_job(worker_key) 
            if job is None:
                try:
                    self._idle_workers.remove(worker_key)
                    logger.info('Worker:%s has been removed from the idle pool'
                            % worker_key)
                    return True
                except ValueError:
                    pass 

            elif job.subtask_key:
                logger.warning('%s failed during task, returning work unit' % worker_key)

                task_instance = self.get_task_instance(job.root_task_id)
                main_worker = self.workers.get(task_instance.main_worker, None)

                if main_worker:
                    # requeue failed work.
                    task_instance.queue_worker_request(
                                    (task_instance.main_worker, job.args,
                                        job.subtask_key, job.workunit_key) )
                
            return False


    def hold_worker(self, worker_key):
        """
        Hold a worker for a task so that it may not be scheduled for other
        jobs.  This allows a Task to maintain control over the same workers,
        reducing costly initialization time.

        @param worker_key: worker to hold
        """
        with self._worker_lock:
            if worker_key not in self._main_workers:
                # we don't need to retain a main worker
                job = self._worker_mappings.get(worker_key, None)
                if job:
                    task_instance = self._active_tasks.get(job.root_task_id, None)
                    if task_instance and worker_key <> task_instance.main_worker:
                        task_instance.running_workers.remove(worker_key)
                        task_instance.waiting_workers.append(worker_key)
                        del self._worker_mappings[worker_key]



    def request_worker(self, requester_key, subtask_key, args, workunit_key):
        """
        Requests a worker for a workunit on behalf of a (main) worker.

        Calling this method means that the worker with key 'requester_key' is a
        main worker.
        """
        job = self.get_worker_job(requester_key)
        if job:
            # mark this worker as a main worker.
            # it will be considered by the scheduler as a special worker
            # resource to complete the task.
            self._main_workers.add(requester_key)

            task_instance = self._active_tasks[job.root_task_id]
            task_instance.queue_worker_request( (requester_key, args,
                        subtask_key, workunit_key) )

            logger.debug('Work Request %s:  sub=%s  args=%s  w=%s ' % (requester_key, subtask_key, args, workunit_key))

            self._schedule()
        else:
            # a worker request from an unknown task
            pass


    def get_worker_job(self, worker_key):
        """
        Returns a WorkerJob object or None if the worker is idle.
        """
        return self._worker_mappings.get(worker_key, None)


    def get_workers_on_task(self, root_task_id):
        """
        Returns a list of keys of those workers working on a specified task.
        """
        task_instance = self._active_tasks.get(root_task_id, None)
        if task_instance is None:
            # finished task or non-existent task
            return []
        else:
            return set([x for x in task_instance.running_workers] + \
                [x for x in task_instance.waiting_workers] +\
                [task_instance.main_worker])


    def get_task_instance(self, task_id):
        task_instance = self._active_tasks.get(task_id, None)
        return TaskInstance.objects.get(id=task_id) if task_instance \
                                           is None else task_instance


    def get_queued_tasks(self):
        return [self._active_tasks[x[1]] for x in self._long_term_queue]


    def get_running_tasks(self):
        return [self._active_tasks[x[1]] for x in self._short_term_queue]


    def get_worker_status(self, worker_key):
        """
        0: idle; 1: working; 2: waiting; -1: unknown
        """
        job = self.get_worker_job(worker_key)
        if job:
            return 1
        elif self._waiting_workers.get(worker_key, None):
            return 2
        elif worker_key in self._idle_workers:
            return 0
        return -1


    def _schedule(self):
        """
        Allocates a worker to a task/subtask.

        Note that a main worker is a special worker resource for executing
        parallel tasks. At the extreme case, a single main worker can finish
        the whole task even without other workers, albeit probably in a slow
        way.
        """

        worker_key, root_task_id, task_key, args, subtask_key, workunit_key = \
                        None, None, None, None, None, None
        on_main_worker = False
        finished_main_workers = []
        with self._queue_lock:
            logger.debug('Attempting to advance scheduler: ltq=%s  stq=%s' % (len(self._long_term_queue), len(self._short_term_queue)))

            if self._short_term_queue:
                task_instance, worker_request = None, None
                for task in self._short_term_queue:
                    root_task_id = task[1]
                    task_instance = self._active_tasks[root_task_id]
                    worker_request = task_instance.poll_worker_request()
                    if worker_request:
                        break
                    #else:
                        ## this task has no pending worker requests
                        ## check if it has workers running or waiting; and
                        ## if not, this task is considered completed
                        #if not task_instance.waiting_workers and \
                                #not task_instance.running_workers:
                            #logger.info('Task %d:%s is finished by %s' %
                                    #(root_task_id, task_instance.task_key,
                                     #task_instance.main_worker))
                            #finished_main_workers.append(task_instance.main_worker)

                if worker_request:
                    with self._worker_lock:
                        requester, args, subtask_key, workunit_key = worker_request
                        task_key = task_instance.task_key                      
                        
                        if subtask_key and task_instance.waiting_workers:
                            # consume waiting worker first
                            task_instance.pop_worker_request()
                            worker_key = task_instance.waiting_workers.pop()
                            logger.info('Re-dispatching waiting worker:%s to %s:%s' % 
                                    (worker_key, subtask_key, workunit_key))
                            task_instance.running_workers.append(worker_key)

                        elif subtask_key and not self._worker_mappings[requester].workunit_key:
                            # the main worker can do a local execution
                            task_instance.pop_worker_request()
                            logger.info('Main worker:%s assigned to task %s:%s' %
                                    (requester, subtask_key, workunit_key))
                            worker_key = requester
                            on_main_worker = True

                        elif self._idle_workers:
                            # dispatching to idle worker last
                            task_instance.pop_worker_request()
                            worker_key = self._idle_workers.pop()
                            task_instance.running_workers.append(worker_key)
                            logger.info('Worker:%s assigned to task=%s, subtask=%s:%s' %
                                    (worker_key, task_instance.task_key, subtask_key, workunit_key))


                else:
                    logger.debug('NO REQUESTS IN QUEUE')


            # satisfy tasks in the ltq only after stq tasks have been satisfied
            if not worker_key and self._long_term_queue:
                with self._worker_lock:
                    if self._idle_workers:
                        # move the task from the ltq to the stq
                        worker_key = self._idle_workers.pop()
                        root_task_id = heappop(self._long_term_queue)[1]
                        task_instance = self._active_tasks[root_task_id]
                        task_instance.last_succ_time = datetime.now()
                        task_key = task_instance.task_key
                        args = simplejson.loads(task_instance.args)
                        subtask_key, workunit_key = None, None
                        heappush(self._short_term_queue,
                                [task_instance.compute_score(), root_task_id])
                        task_instance.main_worker = worker_key
                        task_instance.status = STATUS_RUNNING
                        task_instance.started = datetime.now()
                        task_instance.save()

                        self._main_workers.add(worker_key)
                        logger.info('Task %d has been moved from ltq to stq' %
                                root_task_id)
            

        if worker_key:
            job = WorkerJob(root_task_id, task_key, args, subtask_key, \
                workunit_key, on_main_worker)
            self._worker_mappings[worker_key] = job

            # notify remote worker to start     
            worker = self.workers[worker_key]
            pkg = self.task_manager.get_task_package(task_key)
            d = worker.remote.callRemote('run_task', task_key, pkg.version,
                    args, subtask_key, workunit_key, task_instance.main_worker,
                    task_instance.id)
            d.addCallback(self.run_task_successful, worker_key, subtask_key)
            d.addErrback(self.run_task_failed, worker_key)            

            return worker_key, root_task_id
        else:
            return None


    def _init_queue(self):
        """
        Initialize the ltq and the stq by reading the persistent store.
        """
        with self._queue_lock:
            queued = TaskInstance.objects.queued()
            running = TaskInstance.objects.running()
            for t in queued:
                self._long_term_queue.append([t.compute_score(), t.id])
                self._active_tasks[t.id] = t
            for t in running:
                self._short_term_queue.append([t.compute_score(), t.id])
                self._active_tasks[t.id] = t


    def _update_queue(self):
        """
        Periodically updates the scores of entries in both the long-term and the
        short-term queue and subsequently re-orders them.
        """
        with self._queue_lock:
            for task in self._long_term_queue:
                task_instance = self._active_tasks[task[1]]
                task[0] = task_instance.compute_score()
            heapify(self._long_term_queue)

            for task_id in self._long_term_queue:
                task_instance = self._active_tasks[task[1]]
                task[0] = task_instance.compute_score()
            heapify(self._short_term_queue)

            reactor.callLater(self.update_interval, self._update_queue)


    def return_work_success(self, results, worker_key):
        """
        Work was sucessful returned to the main worker
        """
        #TODO this should be called directly as the success function
        scheduler.remove_worker(worker_key)


    def return_work_failed(self, results, worker_key):
        """
        A worker disconnected and the method call to return the work failed
        """
        #TODO this should add work request
        pass

    
    def queue_task(self, task_key, args={}):
        """
        Queue a task to be run.  All task requests come through this method.

        Successfully queued tasks will be saved in the database.  If the cluster 
        has idle resources it will start the task immediately, otherwise it will 
        remain in the queue until a worker is available.
        
        @param args: should be a dictionary of values.  It is acceptable for
        this to be improperly typed data.  ie. Integer given as a String. This
        function will parse and clean the args using the form class for the Task

        """
        # args coming from the controller need to be parsed by the form. This
        # will give proper typing to the data and allow validation.
        if args:
            task = self.registry[task_key]
            form_instance = task.form(args)
            if form_instance.is_valid():
                # repackage cleaned data
                args = {}
                for key, val in form_instance.cleaned_data.items():
                    args[key] = val

            else:
                # not valid, report errors.
                return {
                    'task_key':task_key,
                    'errors':form_instance.errors
                }

        task_instance = self._queue_task(task_key, args)

        return {
                'task_key':task_key,
                'instance_id':task_instance.id,
                'time':time.mktime(task_instance.queued.timetuple())
               }
    




    def run_task_failed(self, results, worker_key):
        # return the worker to the pool
        self.add_worker(worker_key)


    def run_task_successful(self, results, worker_key, subtask_key=None):
        # save the history of what workers work on what task/subtask
        # its needed for tracking finished work in ParallelTasks and will aide
        # in Fault recovery it might also be useful for analysis purposes 
        # if one node is faulty

        job = self.get_worker_job(worker_key)
        task_instance = self._active_tasks[job.root_task_id]

        if job and task_instance:
            if subtask_key:
                work_unit = WorkUnit()
                work_unit.task_instance = task_instance
                work_unit.subtask_key = subtask_key
                work_unit.workunit_key = job.workunit_key
                work_unit.args = simplejson.dumps(job.args)
                work_unit.started = datetime.now()
                work_unit.worker = worker_key
                work_unit.status = STATUS_RUNNING
                work_unit.save()
                job.model = work_unit

            else:
                task_instance.worker = worker_key
                task_instance.save()


    def send_results(self, worker_key, results, workunit_key):
        """
        Called by workers when they have completed their task.

            Tasks runtime and log should be saved in the database
        """
        logger.debug('Worker:%s - sent results: w=%s results=%s' % \
            (worker_key, workunit_key, results))
        # TODO: this lock does not appear to be sufficient because all of the
        # other functions use specific locks, might need to obtain both locks
        with self._lock:
            job = self.get_worker_job(worker_key)
            logger.info('Worker:%s - completed: %s:%s (%s)' %  \
                (worker_key, job.task_key, job.subtask_key, job.workunit_key))

            # check to make sure the task was still in the queue.  Its possible
            # this call was made at the same time a task was being canceled.  
            # Only worry about sending the results back to the Task Head 
            # if the task is still running
            if job.subtask_key:

                task_instance = self.get_task_instance(job.root_task_id)

                # if this was a subtask the main task needs the results and to 
                # be informed
                main_worker = self.workers[task_instance.main_worker]
                logger.debug('Worker:%s - informed that subtask completed' %
                        main_worker.name)
                main_worker.remote.callRemote('receive_results', worker_key,
                        results, job.subtask_key, job.workunit_key)

                # Clear attributes if mainworker. This prevents bugs later on
                # with scheduling and task completion if not cleared
                if job.on_main_worker:
                    job.on_main_worker = False
                    job.workunit_key = None
                    job.subtask_key = None

                else:
                    # Hold this worker for the next workunit or mainworker
                    # releases it.
                    self.hold_worker(worker_key)

                # advance the scheduler if there is already a request waiting 
                # for this task, otherwise there will be nothing to advance.
                if len(task_instance._worker_requests) != 0:
                    threads.deferToThread(self._schedule)

                # save information about this workunit to the database
                work_unit = job.model
                work_unit.completed = datetime.now()
                work_unit.status = STATUS_COMPLETE
                work_unit.save()

            else:
                # this is the root task, so we can return the worker to the
                # idle pool
                logger.info("Root task:%s completed by worker:%s" %
                        (job.task_key, worker_key))
                self.add_worker(worker_key)



    def task_failed(self, worker_key, results, workunit_key):
        """
        Called by workers when the task they were running throws an exception
        """
        with self._lock:
            job = self.get_worker_job(worker_key)
            if job is not None:
                logger.info('Worker:%s - failed: %s:%s (%s)' % (worker_key,
                        job.task_key, job.subtask_key, job.workunit_key))
            self.add_worker(worker_key, STATUS_FAILED)

            # cancel the task and send notice to all other workers to stop
            # working on this task.  This may be partially recoverable but that
            # is not included for now.
            for other_worker_key in self.get_workers_on_task(job.root_task_id):
                if other_worker_key != worker_key:
                    worker = self.workers[other_worker_key]
                    logger.debug('signalling worker to stop: %s' % worker_key)
                    worker.remote.callRemote('stop_task')


    def worker_stopped(self, worker_key):
        """
        Called by workers when they have stopped due to a cancel task request.
        """
        logger.info(' Worker:%s - stopped' % worker_key)
        self.add_worker(worker_key, STATUS_CANCELLED)


    def request_worker_release(self, worker_key):
        """
        Release a worker held by the worker calling this function.

        @param worker_key: worker signally that it does not have additional
                           workunits but is holding a worker.
        """

        # select the best worker to release.  For now just release the first
        # worker in the waiting worker list
        logger.debug('[%s] request worker release' % worker_key)
        released_worker_key = None
        job = self._worker_mappings.get(worker_key, None)
        if job:
            task_instance = self._active_tasks.get(job.root_task_id, None)
            if task_instance:
                released_worker_key = task_instance.waiting_workers.pop()

        if released_worker_key:
            logger.debug('Task %s - releasing worker: %s' % \
                    (worker_key, released_worker_key))
            worker = self.workers[released_worker_key]
            worker.remote.callRemote('release_worker')
            self.add_worker(released_worker_key)


    def worker_connected(self, worker_avatar):
        """
        Callback when a worker has been successfully authenticated
        """
        #request status to determine what this worker was doing
        deferred = worker_avatar.remote.callRemote('worker_status')
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
        self.add_worker(worker_key)


    def fetch_task_status(self):
        """
        updates the list of statuses.  this function is used because all
        workers must be queried to receive status updates.  This results in a
        list of deferred objects.  There is no way to block until the results
        are ready.  instead this function updates all the statuses. Subsequent
        calls for status will be able to fetch the status.  It may be delayed 
        by a few seconds but thats minor when a task could run for hours.

        For now, statuses are only queried for Main Workers.  Including 
        statuses of subtasks requires additional logic and overhead to pass the
        intermediate results to the main worker.
        """

        # limit updates so multiple controllers won't cause excessive updates
        now = datetime.now()
        if self._next_task_status_update < now:
            for key, worker in self.workers.items():
                if self.get_worker_status(key) == 1:
                    data = self.get_worker_job(key)
                    if data.subtask_key and not data.on_main_worker:
                        continue
                    task_instance_id = data.root_task_id
                    deferred = worker.remote.callRemote('task_status')
                    deferred.addCallback(self.fetch_task_status_success, \
                    task_instance_id)
            self.next_task_status_update = now + timedelta(0, 3)


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
        for instance in self.get_queued_tasks():
            statuses[instance.id] = {'s':STATUS_STOPPED}

        for instance in self.get_running_tasks():
            start = time.mktime(instance.started.timetuple())

            # call worker to get status update
            try:
                progress = self._task_statuses[instance.id]

            except KeyError:
                # its possible that the progress does not exist yet. Because
                # the task has just started and fetch_task_status is not 
                # complete
                pass
                progress = -1

            statuses[instance.id] = {'s':STATUS_RUNNING, 't':start, 'p':progress}

        return statuses
