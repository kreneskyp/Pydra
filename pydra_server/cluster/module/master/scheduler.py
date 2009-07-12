from pydra_server.cluster.module import Module

class TaskScheduler(Module):

    _signals = [
        'TASK_QUEUED',
        'TASK_STARTED',
        'TASK_FAILED',
        'TASK_FINISHED',
        'WORKUNIT_REQUESTED',
        'WORKUNIT_COMPLETED',
        'WORKUNIT_FAILED'
    ]

