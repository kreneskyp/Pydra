from __future__ import with_statement
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

import logging
import os
from threading import Lock
import zlib
logger = logging.getLogger('root')

from django.db.models import Q

import pydra_settings
from logger import task_log_path
from pydra.models import TaskInstance, WorkUnit
from pydra.cluster.module import Module
from pydra.cluster.tasks import STATUS_CANCELLED, STATUS_FAILED, STATUS_COMPLETE
from pydra.util import makedirs

STATUSES = (STATUS_FAILED, STATUS_CANCELLED, STATUS_COMPLETE)

class MasterLogAggregator(Module):
    """
    Module for aggregating logs from tasks run on different nodes in the
    cluster.  This includes physically transfering logs to the master
    and aggregating the logs into a cohesive form once its there
    """

    _shared = [
        'workers',
        'nodes'
    ]

    def __init__(self):

        self._interfaces = [
            self.aggregate_task_logs
        ]
        
        self._listeners = {
            'CLUSTER_IDLE':self.aggregate_logs
        }

        self.task_iteration_lock = Lock()
        self.transfer_lock = Lock()
        self.transfers = []


    def aggregate_logs(self, workers=None):
        """
        Scans TaskInstances and their Workunits and selects logs to aggregate.

        This function synchronizes querying and iteration of tasks and workunits
        with pending log transfers.  This is required because as each worker
        becomes idle.  It will trigger a call to this function. A full iteration
        will occur before the next call is processed, reducing the number of
        times a task might be processed.
        
        @param workers [None] - list of idle workers.  Logs are only requested
                                from idle workers.  If None, logs are retrieved
                                from all workers
        """
        with self.task_iteration_lock:
            logger.info('Cluster Idle, aggregating all logs')
            query = TaskInstance.objects \
                        .filter(Q(log_retrieved=False) \
                                | Q(workunits__log_retrieved=False)) \
                        .order_by('completed') \
                        .distinct()
            for task in query:
                self.aggregate_task_logs(task)
    
    
    def aggregate_task_logs(self, task, workers=None):
        """
        aggregates all logs for a give task or workunit.  Logs are not
        retrieved if the task or workunit is still running.  Completed workunits
        for a running task will be retrieved
        
        @param workers [None] - list of idle workers.  Logs are only requested
                                from idle workers.  If None, logs are retrieved
                                from all workers
        """
        if isinstance(task, (int,)):
            task = TaskInstance.objects.get(task)
            
        # aggregate main task log
        if not task.log_retrieved and task.status in STATUSES:
            if not workers or task.worker in workers:
                self.aggregate_log(task.worker, task.id)

        # aggregate workunit logs, if any
        for wu in task.workunits.filter(log_retrieved=False) \
                                    .filter(status__in=STATUSES):
            if not workers or wu.worker in workers:
                self.aggregate_log(wu.worker, task.id, wu.subtask_key, \
                               wu.workunit)


    def aggregate_log(self, worker, task, subtask=None, workunit=None):
        """
        Aggregates a single log file.  Either the main task log, or a subtask
        log
        
        This function synchronizes per file download requests to prevent excess
        requests for the same file.
        
        @param worker - id of worker sending the log
        @param task - id of task
        @param subtask - task path for subtask, default=None
        @param workunit - id of workunit, default=None
        """
        if worker not in self.workers:
            logger.debug("Can't aggregate logs for unknown worker %s" % worker)
            return

        key = (task, subtask, workunit)
        with self.transfer_lock:
            if key in self.transfers:
                return
            else:
                self.transfers.append(key)
        logger.debug('Requesting Log: %s %s %s' % key)
        remote = self.workers[worker]
        deferred = remote.callRemote('send_log', task, subtask, workunit)
        deferred.addCallback(self.receive_log, worker, \
                             task, subtask, workunit)


    def receive_log(self, response, worker, task, subtask=None, workunit=None):
        """
        Callback from a successful call to Node.send_log.  Saves the log
        locally, updates state, and deletes the remote log.
        
        @param worker - id of worker sending the log
        @param task - id of task
        @param subtask - task path for subtask, default=None
        @param workunit - id of workunit, default=None
        """
        logger.debug('Receiving Log: %s %s %s' % (task, subtask, workunit))

        if not response:
            logger.debug("Bogus log: %s %s %s" % (task, subtask, workunit))
            return
        log = zlib.decompress(response)

        d, path = task_log_path(task, subtask, workunit)

        try:
            makedirs(d)
        except OSError:
            # Couldn't make our path to our log; give up.
            logger.debug("Couldn't recieve log: %s %s %s"
                % (task, subtask, workunit))
            return
        out = open(path, 'w')
        out.write(log)
        
        # mark log as received
        if workunit:
            item = WorkUnit.objects.get(task_instance__id=task, \
                                        workunit=workunit)
        else:
            item = TaskInstance.objects.get(id=task)
        item.log_retrieved = True
        item.save()

        # delete remote log
        remote = self.workers[worker]
        remote.callRemote('delete_log', task, subtask, workunit)


class NodeLogAggregator(Module):
    """
    Module for aggregating logs from tasks run on different nodes in the
    cluster.  This is the Node side of the process and includes functions for
    retrieving and deleting local logs
    """

    _shared = ['worker_key']

    def __init__(self):
        self._remotes = [
            ('MASTER', self.send_log),
            ('MASTER', self.delete_log)
        ]

        self.delete_lock = Lock()

    def send_log(self, master, worker, task, subtask=None, workunit=None):
        """
        Sends the requested log file
        
        @param worker - id of worker
        @param task - id of task
        @param subtask - task path for subtask, default=None
        @param workunit - id of workunit, default=None
        """
        d, path = task_log_path(task, subtask, workunit, worker)
        try:
            logger.debug('Sending Log: %s' % path)
            log = open(path, 'r')
            text = log.read()
            compressed = zlib.compress(text)
            return compressed
        except IOError:
            logger.debug("Couldn't send log: %s" % path)
    
    
    def delete_log(self, master, worker, task, subtask=None, workunit=None):
        """
        deletes the requested log file
        
        @param worker - id of worker
        @param task - id of task
        @param subtask - task path for subtask, default=None
        @param workunit - id of workunit, default=None
        """
        d, path = task_log_path(task, subtask, workunit, worker)
        logger.debug('Deleting Log: %s' % path)
        # synchronize deletes to ensure directory gets deleted
        with self.delete_lock:
            try:
                # Remove the log and any directories emptied in the process
                os.remove(path)
                os.removedirs(d)
            except os.error:
                pass
