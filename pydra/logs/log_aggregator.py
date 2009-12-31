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
            #'CLUSTER_IDLE':self.aggregate_logs
        }

        self.transfer_lock = Lock()
        self.transfers = []


    def aggregate_logs(self):
        """
        Scans TaskInstances and their Workunits and selects logs to aggregate.

        For now this aggregates all logs in order of descending age.  At some
        point this should be updated to pull from idle workers.
        """
        logger.info('Cluster Idle, aggregating all logs')
        query = TaskInstance.objects \
                    .filter(Q(log_retrieved=False) | Q(workunits__log_retrieved=False)) \
                    .order_by('completed')
        for task in query:
            self.aggregate_task_logs(task)
    
    
    def aggregate_task_logs(self, task):
        """
        aggregates all logs for a give task or workunit.  Logs are not
        retrieved if the task or workunit is still running.  Completed workunits
        for a running task will be retrieved
        """
        if isinstance(task, (int,)):
            task = TaskInstance.objects.get(task)
            
        # aggregate main task log
        if not task.log_retrieved and task.status in STATUSES:
            self.aggregate_log(task.worker, task.id)

        # aggregate workunit logs, if any
        for wu in task.workunits.filter(log_retrieved=False) \
                                    .filter(status__in=STATUSES):
            self.aggregate_log(wu.worker, task.id, wu.subtask_key, \
                               wu.workunit_key)


    def aggregate_log(self, worker, task, subtask=None, workunit=None):
        """
        Aggregates a single log file.  Either the main task log, or a subtask
        log
        
        @param worker - id of worker sending the log
        @param task - id of task
        @param subtask - task path for subtask, default=None
        @param workunit - id of workunit, default=None
        """
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
        log = zlib.decompress(response)
        dir, path = task_log_path(task, subtask, workunit)
        
        if os.path.exists(path):
            os.remove(path)
        
        if not os.path.exists(dir):
            os.mkdir(dir)
            
        out = open(path ,'w')
        out.write(log)
        
        # mark log as received
        if workunit:
            item = Workunit.objects.get(workunit)
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


    def send_log(self, master, worker, task, subtask=None, workunit=None):
        """
        Sends the requested log file
        
        @param worker - id of worker
        @param task - id of task
        @param subtask - task path for subtask, default=None
        @param workunit - id of workunit, default=None
        """
        dir, path = task_log_path(task, subtask, workunit, worker)
        if os.path.exists(path):
            logger.debug('Sending Log: %s' % path)
            log = open(path, 'r')
            text = log.read()
            compressed = zlib.compress(text)
            return compressed
        else:
            raise Exception(path)
    
    
    def delete_log(self, master, worker, task, subtask=None, workunit=None):
        """
        deletes the requested log file
        
        @param worker - id of worker
        @param task - id of task
        @param subtask - task path for subtask, default=None
        @param workunit - id of workunit, default=None
        """
        dir, path = task_log_path(task, subtask, workunit, worker)
        if os.path.exists(path):
            logger.debug('Deleting Log: %s' % path)
            #os.remove(path)