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
from twisted.internet.defer import Deferred, DeferredList

from pydra.cluster.module import Module
from pydra.cluster.tasks import *
from pydra.cluster.tasks.task_manager import TaskManager
from pydra.cluster.constants import *
from pydra.models import TaskInstance, WorkUnit

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

    def __init__(self):

        self._listeners = {
            'WORKER_DISCONNECTED':self.remove_worker,
            'WORKER_CONNECTED':self.worker_connected,
            'CANCEL_TASK': self.cancel_task,
        }

        self._remotes = [
            ('NODE', self.request_worker),
            ('NODE', self.send_results),
            ('NODE', self.worker_stopped),
            ('NODE', self.request_worker_release)
        ]

        self._friends = {
            'task_manager' : TaskManager,
        }

        self._interfaces = [
            (self.fetch_task_status, {'name':'task_statuses'}),
            self.cancel_task,
            self.queue_task,
            (self.get_queued_tasks, {'name':'list_queue'}),
        ]

        # locks
        self._lock = Lock()         # general lock        
        self._worker_lock = Lock()  # lock for worker only transactions
        self._queue_lock = Lock()   # lock for queue only transactions
        self._fetch_status_lock = Lock() # lock for retrieving statuses

        # task statuses
        self._task_statuses = {}
        self._next_task_status_update = datetime.now()

        # a set containing all main workers
        self._main_workers = set()

        self.update_interval = 5 # seconds


    def _register(self, manager):
        Module._register(self, manager)
        
        self._queue = []
        self._active_tasks = {}     # caching uncompleted task instances
        self._idle_workers = []     # all workers are seen equal
        self._active_workers = {}   # worker-job mappings
        self._waiting_workers = {}  # task-worker mappings
        
        self._init_queue()
        reactor.callLater(self.update_interval, self._update_queue)

    def _queue_task(self, task_key, args={}, priority=5):
        """
        Adds a (root) task that is to be run.

        Under the hood, the scheduler creates a task instance for the task, puts
        it into the queue, and then tries to advance the queue.
        """
        logger.info('Queued Task: %s - Args:  %s' % (task_key, args))

        task_instance = TaskInstance()
        task_instance.task_key = task_key
        task_instance.priority = priority
        task_instance.args = simplejson.dumps(args)
        task_instance.queued = datetime.now()
        task_instance.status = STATUS_STOPPED
        task_instance.save()
        
        # queue the root task as the first work request.  This lets the queue
        # advancement logic to function the same for a root task or a subtask
        task_instance.queue_worker_request(task_instance)
        
        with self._queue_lock:
            heappush(self._queue, [task_instance.compute_score(),task_instance])
            # cache this task
            self._active_tasks[task_instance.id] = task_instance

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
            task = self._active_tasks.get(task_id)
            self._queue.remove([task.compute_score(), task])
            # cancel any workers assigned to the task.  task is not
            # marked cancelled until all workers have reported they
            # stopped
            task_workers = self.get_workers_on_task(task_id)
            if task_workers:
                for worker_key in task_workers:
                    worker = self.workers[worker_key]
                    logger.debug('Signalling Stop: %s' % worker_key)
                    worker.remote.callRemote('stop_task')
            else:
                # this worker was waiting for workers, just mark it
                # cancelled
                task = self._active_tasks[task_id]
                del self._active_tasks[task_id]
                task.status = STATUS_CANCELLED
                task.completed = datetime.now()
                task.save()
        return task != None


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

        job = self._active_workers.get(worker_key, None)
        if job:
            task_instance = job.task_instance
            
            if worker_key in self._main_workers:
                # this is a main worker
                if not job.local_workunit or task_status == STATUS_CANCELLED:
                    logger.info('Main worker:%s finishes the root task' %
                            worker_key)
                    
                    status = STATUS_COMPLETE if task_status is None else task_status
                    task_instance.status = status
                    task_instance.completed = datetime.now()
                    task_instance.save()
                    
                    with self._worker_lock:
                        self._main_workers.remove(worker_key)
                        self._idle_workers.append(worker_key)
                        del self._active_workers[worker_key]
                        
                    with self._queue_lock:
                        del self._active_tasks[job.task_id]
                        if status in (STATUS_CANCELLED, STATUS_COMPLETE, STATUS_FAILED):
                            # safe to remove the task
                            # release any unreleased workers
                            for key in task_instance.waiting_workers:
                                avatar = self.workers[key]
                                avatar.remote.callRemote('release_worker')

                            t = [job.compute_score(), job]
                            if t in self._queue:
                                self._queue.remove(t)
                                logger.info(
                                    'Task %d: %s is removed from the queue' % \
                                    (job.task_id, job.task_key))

            else:
                # not a main worker
                logger.info("Task %d returns a worker: %s" % (job.task_id,
                            worker_key))
                if job.subtask_key is not None:
                    with self._worker_lock:
                        del self._active_workers[worker_key]
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

        @returns True if this operation succeeds and False otherwise.
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

                task_instance = job.task_instance
                main_worker = self.workers.get(task_instance.worker, None)

                if main_worker:
                    # requeue failed work.
                    task_instance.queue_worker_request(job)
                
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
                job = self._active_workers.get(worker_key, None)
                if job:
                    task_instance = job.task_instance
                    task_instance.running_workers.remove(worker_key)
                    task_instance.waiting_workers.append(worker_key)
                    del self._active_workers[worker_key]



    def request_worker(self, requester_key, subtask, args, workunit):
        """
        Requests a worker for a workunit on behalf of a (main) worker.
        
        @param requester_key - key of worker requesting work.  The requester
                            can only request work for the task it is currently
                            assigned
        @param subtask - key of the subtask to run
        @param args - arguments to pass to the task
        @param workunit - key that will retrieve additional data for this
                            workunit.
        """
        task_instance = self.get_worker_job(requester_key)
        if task_instance:
            # mark this worker as a main worker.
            # it will be considered by the scheduler as a special worker
            # resource to complete the task.
            self._main_workers.add(requester_key)

            job = WorkUnit()
            job.task_instance = task_instance
            job.subtask_key = subtask
            job.args = simplejson.dumps(args)
            job.workunit = workunit
            job.save()

            task_instance.queue_worker_request(job)
            logger.debug('Work Request %s:  sub=%s  args=%s  w=%s ' % \
                         (requester_key, subtask, '--', workunit))

            self._schedule()
        else:
            # a worker request from an unknown task
            pass


    def get_worker_job(self, worker_key):
        """
        Returns a WorkerJob object or None if the worker is idle.
        """
        return self._active_workers.get(worker_key, None)


    def get_workers_on_task(self, task_id):
        """
        Returns a list of keys of those workers working on a specified task.
        This includes the main worker, workers running workunits, and workers
        that are waiting for more work from the root task.
        
        @param task_id - ID of task
        """
        task_instance = self._active_tasks.get(task_id, None)
        if task_instance is None or not task_instance.worker:
            # finished task or non-existent task
            return []
        else:
            return set([x for x in task_instance.running_workers] + \
                [x for x in task_instance.waiting_workers] +\
                [task_instance.worker])


    def get_task_instance(self, task_id):
        task_instance = self._active_tasks.get(task_id, None)
        return TaskInstance.objects.get(id=task_id) if task_instance \
                                           is None else task_instance


    def get_queued_tasks(self, json_safe=True):
        """
        Returns list of tasks in the queue.  This includes running tasks and
        tasks waiting for a worker.
        
        @param json_safe [True] - return only primitives, lists, and dicts. 
        """
        if json_safe:
            return [task[1].json_safe() for task in self._queue]
        return [task[1] for task in self._queue]

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
        
        If no tasks are in the queue or no job is in the queue CLUSTER_IDLE is
        emited
        """

        task, subtask, workunit = None, None, None
        
        finished_main_workers = []
        with self._queue_lock:
            logger.debug('Attempting to advance scheduler: q=%s' % (len(self._queue)))
            
            if self._queue:
                # find taskinstance or a worker_request
                task_instance, job = None, None
                for item in self._queue:
                    job = item[1].poll_worker_request()
                    if job:
                        task_instance = item[1]
                        break
                
                if job:
                    with self._worker_lock:
                        worker_key = None
                        task = task_instance.task_key
                        subtask = job.subtask_key
                        if subtask and task_instance.waiting_workers:
                            # consume waiting worker first
                            worker_key = task_instance.waiting_workers.pop()
                            logger.info('Re-dispatching waiting worker:%s to task:%s' % 
                                    (worker_key, task_instance.id))
                            task_instance.running_workers.append(worker_key)

                        elif subtask and not task_instance.local_workunit:
                            # the main worker can do a local execution
                            worker_key = task_instance.worker
                            logger.info('Main worker:%s assigned to task:%s' %
                                    (worker_key, task_instance.id))

                        elif self._idle_workers:
                            # dispatching to idle worker last
                            worker_key = self._idle_workers.pop()
                            task_instance.running_workers.append(worker_key)
                            logger.info('Worker:%s assigned to task:%s  key=%s' %
                                    (worker_key, task_instance.id, task))

                    # was a worker found for the job
                    if worker_key:
                        job = task_instance.get_batch()
                        job.worker = worker_key
                        
                        if not (subtask and job.on_main_worker):
                            self._active_workers[worker_key] = job
                        else:
                            task_instance.local_workunit = job

                        # notify remote worker to start     
                        worker = self.workers[worker_key]
                        pkg = self.task_manager.get_task_package(task)
                        main_worker = task_instance.worker if task_instance.worker else worker_key
                        d = worker.remote.callRemote('run_task', task, pkg.version,
                                job.args, job.transmitable(), main_worker,
                                task_instance.id)
                        d.addCallback(self.run_task_successful, worker_key, subtask)
                        d.addErrback(self.run_task_failed, worker_key)            
            
                        return worker_key, job.task_id

                else:
                    self.emit('CLUSTER_IDLE', self._idle_workers)
            else:
                self.emit('CLUSTER_IDLE', self._idle_workers)
                    
        return None


    def _init_queue(self):
        """
        Initialize the queue by reading the persistent store.
        """
        with self._queue_lock:
            queued = TaskInstance.objects.queued()
            running = TaskInstance.objects.running()
            for t in running:
                self._queue.append([t.compute_score(), t.id])
                self._active_tasks[t.id] = t
            for t in queued:
                self._queue.append([t.compute_score(), t.id])
                self._active_tasks[t.id] = t


    def _update_queue(self):
        """
        Periodically updates the scores of entries in both the long-term and the
        short-term queue and subsequently re-orders them.
        """
        with self._queue_lock:
            for task in self._queue:
                task[0] = task[1].compute_score()
            heapify(self._queue)
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
        if job:
            if subtask_key:
                # notify main worker that a subtask was started
                worker = self.workers[job.task_instance.worker].remote
                if isinstance(job, (TaskInstance,)):
                    job = job.local_workunit
                worker.callRemote('subtask_started', job.transmitable())
            else:
                self._main_workers.add(worker_key)
                job.last_succ_time = datetime.now()
                
            job.worker = worker_key
            job.status = STATUS_RUNNING
            job.started = datetime.now()
            job.save()


    def send_results(self, worker_key, results):
        """
        Called by workers when they have completed their task.

            Tasks runtime and log should be saved in the database
        """
        logger.debug('Worker:%s - sent results' % worker_key)
        # TODO: this lock does not appear to be sufficient because all of the
        # other functions use specific locks, might need to obtain both locks
        with self._lock:
            job = self.get_worker_job(worker_key)

            # check to make sure the task was still in the queue.  Its possible
            # this call was made at the same time a task was being canceled.  
            # Only worry about sending the results back to the Task Head 
            # if the task is still running
            if job:
                task_instance = job.task_instance
                if results[0][0]:
                    if isinstance(job, (TaskInstance)):
                        job = job.local_workunit
                        task_instance.local_workunit = None
                    else:
                        # Hold this worker for the next workunit or mainworker
                        # releases it.
                        self.hold_worker(worker_key)

                    # advance the scheduler if there is a request waiting 
                    # for this task, otherwise there will be nothing to advance.
                    # this reassigns the waiting worker quickly.
                    if len(task_instance._worker_requests) != 0:
                        threads.deferToThread(self._schedule)
    
                    # if this was a subtask the main task needs the results and to 
                    # be informed
                    main_worker = self.workers[task_instance.worker]
                    logger.debug('Worker:%s - informed that subtask completed' %
                            main_worker.name)
                    
                    main_worker.remote.callRemote('receive_results', worker_key,
                            results, job.subtask_key)
    
                    # save information about the workunits to the database
                    now = datetime.now()
                    if len(results) > 1:
                        for workunit_key, results, failed in results:
                            status_msg = 'failed' if failed else 'completed'
                            workunit = job[workunit_key]
                            status = STATUS_FAILED if failed else STATUS_COMPLETE
                            logger.info('Worker:%s - %s: %s:%s (%s)' %  \
                                (status_msg, worker_key, job.task_key, \
                                workunit.subtask_key, workunit_key))
                            workunit.completed = now
                            workunit.status = status
                            workunit.save()
                        job.size = len(results)
                        job.status = STATUS_COMPLETE
                        job.completed = now
                        job.save()
                    else:
                        status_msg = 'failed' if results[0][2] else 'completed'
                        logger.info('Worker:%s - %s: %s:%s (%s)' %  \
                            (status_msg, worker_key, job.task_key, \
                            job.subtask_key, results[0][1]))
                        status = STATUS_FAILED if results[0][2] else STATUS_COMPLETE
                        job.status = status
                        job.completed = now
                        job.save()
    
                else:
                    # this is the root task, so we can return the worker to the
                    # idle pool
                    logger.info("Root task:%s completed by worker:%s" %
                            (job.task_key, worker_key))
                    status = STATUS_FAILED if results[0][2] else STATUS_COMPLETE
                    self.add_worker(worker_key, status)
                    job.results = results[0][1]
                    job.save()


    def worker_stopped(self, worker_key):
        """
        Called by workers when they have stopped due to a cancel task request.
        """
        job = self.get_worker_job(worker_key)
        if job.subtask_key:
            # save information about this workunit to the database
            job.completed = datetime.now()
            job.status = STATUS_CANCELLED
            job.save()
        
        logger.info(' Worker:%s - stopped' % worker_key)
        self.add_worker(worker_key, STATUS_CANCELLED)


    def request_worker_release(self, worker_key):
        """
        Release a worker held by the worker calling this function.

        This should eventually use logic to intelligently select the worker to
        release.  For now it just releases the first worker in the list.
        
        @param worker_key: worker signally that it does not have additional
                           workunits but is holding a worker.
        """
        logger.debug('[%s] request worker release' % worker_key)
        released_worker_key = None
        job = self._active_workers.get(worker_key, None)
        if job:
            released_worker_key = job.task_instance.waiting_workers.pop()

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
        updates the list of task statuses, a dictionary of task status and
        progress.
        
        This function is a combination of tabulating statuses from queued task
        objects and calls to workers.

        For now, statuses are only queried for Main Workers.  Including 
        statuses of subtasks requires additional logic and overhead to pass the
        intermediate results to the main worker.
        
        @returns the current cached statuses, or a deferred if the statuses
                are still being fetched
        """

        # cache updates so multiple controllers won't cause excessive calls
        # to the workers.  While a status update is in progress _task_statuses
        # will be set to deferred so all requests will return the new statuses
        # as soon as they are returned
        now = datetime.now()
        with self._fetch_status_lock:
            if self._next_task_status_update > now:
                return self._task_statuses
            statuses_deferred = Deferred()
            self._task_statuses = statuses_deferred
        self.next_task_status_update = now + timedelta(0, 3)
        
        deferreds = []
        statuses = {}
        for priority, task in self._queue:
            if task.status == STATUS_STOPPED:
                statuses[task.id] = {'s':STATUS_STOPPED}
            else:
                start = time.mktime(task.started.timetuple())
                status = {'s':task.status, 't':start, 'p':-1}
                statuses[task.id] = status
                worker = self.workers[task.worker]
                deferred = worker.remote.callRemote('task_status')
                deferred.addCallback(self.fetch_task_status_success, status)
                deferreds.append(deferred)

        # Possible that there are only queued tasks, or nothing in the queue. In
        # which case no deferreds from the worker will be pending, immediately 
        # call the success method
        if deferreds:
            deferred_list = DeferredList(deferreds, consumeErrors=True)
            deferred_list.addCallback(self.fetch_task_statuses_success,statuses)
            return deferred_list
        return self.fetch_task_statuses_success(None, statuses)


    def fetch_task_status_success(self, progress, status):
        """
        updates task status list with response from worker used in conjunction
        with fetch_task_status()
        
        @param progress - result from remote call to task_status.  progress
                        completed as an integer 0 - 100
        @param status - dictionary containing status of task
        """
        status['p'] = progress


    def fetch_task_statuses_success(self, results, statuses):
        """
        Called when all workers have reported their status.  Saves the final
        list of results as the current list and returns it.  This also triggers
        the callback for any deferreds waiting for completion
        
        @param results - results from deferred list
        @param statuses - completed list of statuses.
        """
        with self._fetch_status_lock:
            self._task_statuses.callback(statuses)
            self._task_statuses = statuses
        return statuses
