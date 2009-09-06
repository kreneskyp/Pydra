
import logging
logger = logging.getLogger('root')

class MasteraLogAggregator(Module):
    """
    client for aggregating logs from tasks run on different nodes in the
    cluster.  This includes physically transfering logs to the master
    and aggregating the logs into a cohesive form once its there
    """

    _shared = [
        'workers',
        'nodes'
    ]

    def __init__(self, manager):

        self._interfaces = [
        ]
        
        Module.__init__(self, manager)


    def aggregate_logs(self):
        """
        Scans TaskInstances and their Workunits and selects logs to aggregate.

        For now this aggregates all logs in order of descending age.  At some
        point this should be updated to pull from idle workers.
        """

        tasks = TaskInstance.objects.filter(log_retrieved=False).order_by('completed','finished')
        
        for task in tasks:
            # aggregate main task log
            if not task.log_retrieved:
                self.aggregate_log(task.worker_key, task_id)

            # aggregate workunit logs if any
            for workunit in task.workunits.filter(log_retrieved=False):
                self.aggregate_log(task.worker_key, task_id, subtask_id, workunit_key)


    def aggregate_log(self, worker_key, task_id, subtask_id=None, workunit_key=None):
        
        

    def aggregate_log_success(self, response, worker_key, task_id, subtask_id=None, workunit_key=None)
