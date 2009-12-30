import logging
from threading import Lock
import zlib
logger = logging.getLogger('root')

import pydra_settings
import task_log_path
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

    def __init__(self, manager):

        self._interfaces = [
            self.aggregate_task_logs
        ]
        
        self._listeners = {
            'CLUSTER_IDLE':self.aggregate_logs
        }
        
        Module.__init__(self, manager)


    def aggregate_logs(self):
        """
        Scans TaskInstances and their Workunits and selects logs to aggregate.

        For now this aggregates all logs in order of descending age.  At some
        point this should be updated to pull from idle workers.
        """
        query = TaskInstance.objects \
                                .filter(log_retrieved=False) \
                                .filter(workunits__log_retrieved=False) \
                                .order_by('completed', 'finished')
        for task in tasks:
            self.aggregate_log(task)
    
    
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
            self.aggregate_log(self.workers[task.worker_key], task_id)

        # aggregate workunit logs, if any
        for wu in task.workunits.filter(log_retrieved=False) \
                                    .filter(status__in=STATUSES):
            worker = self.workers[wu.worker_key]
            self.aggregate_log(worker, task.id, wu.subtask_key, wu.workunit_key)


    def aggregate_log(self, worker, task, subtask=None, workunit=None):
        """
        Aggregates a single log file.  Either the main task log, or a subtask
        log
        
        @param worker - id of worker sending the log
        @param task - id of task
        @param subtask - task path for subtask, default=None
        @param workunit - id of workunit, default=None
        """
        remote = worker[worker]
        deferred = worker.callRemote('send_log', task, subtask, workunit)
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
        log = zlib.decompress(response)
        path = task_log_path(task, subtask, workunit)
        
        if os.path.exists(path):
            os.remove(path)
            
        out = open(path ,'w')
        file.write(log)
        
        # mark log as received
        if workunit_key:
            item = Workunit.objects.get(workunit)
        else:
            item = Taskinstance.objects.get(task)
        item.log_retrieved = True
        item.save()

        # delete remote log
        remote = self.workers[worker_key]
        remote.callRemote('delete_log', task, subtask, workunit)


class NodeLogAggregator:
    """
    Module for aggregating logs from tasks run on different nodes in the
    cluster.  This is the Node side of the process and includes functions for
    retrieving and deleting local logs
    """

    _shared = ['worker_key']

    def __init__(self, manager):

        self._remotes = [
            ('MASTER', self.send_log)
            ('MASTER', self.delete_log)
        ]

        Module.__init__(self, manager)


    def send_log(self, task, subtask=None, workunit=None):
        """
        Sends the requested log file
        
        @param task - id of task
        @param subtask - task path for subtask, default=None
        @param workunit - id of workunit, default=None
        """
        path = task_log_path(task, subtask, workunit, self.worker_key)
        if os.path.exists(path):
            log = open(path, 'r')
            text = log.read()
            compressed = zlib.compress(text)
            return compressed
    
    
    def delete_log(self, task, subtask=None, workunit=None):
        """
        deletes the requested log file
        
        @param task - id of task
        @param subtask - task path for subtask, default=None
        @param workunit - id of workunit, default=None
        """
        path = task_log_path(task, subtask, workunit, self.worker_key)
        if os.path.exists(path):
            #os.remove(path)