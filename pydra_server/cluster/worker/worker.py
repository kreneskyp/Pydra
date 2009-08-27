#! /usr/bin/python

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

#
# Setup django environment
#
if __name__ == '__main__':
    import sys
    import os

    #python magic to add the current directory to the pythonpath
    sys.path.append(os.getcwd())

    #
    if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
        os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'


from twisted.internet import reactor

from pydra_server.cluster.module import ModuleManager
from pydra_server.cluster.tasks.task_manager import TaskManager
from pydra_server.cluster.worker import WorkerTaskControls, WorkerConnectionManager

# TaskSyncClient will eventually migrate to the node
from pydra_server.cluster.node.task_sync import TaskSyncClient


# init logging
import settings
from pydra_server.logging.logger import init_logging
logger = init_logging(settings.LOG_FILENAME_NODE)


class Worker(ModuleManager):
    """
    Worker - The Worker is the workhorse of the cluster.  It sits and waits for Tasks and SubTasks to be executed
            Each Task will be run on a single Worker.  If the Task or any of its subtasks are a ParallelTask 
            the first worker will make requests for work to be distributed to other Nodes
    """
    def __init__(self, port, worker_key):

        self.master_port = port
        self.worker_key = worker_key

        self.modules = [
            TaskManager,
            WorkerConnectionManager,
            WorkerTaskControls,
            TaskSyncClient,
        ]

        ModuleManager.__init__(self)

        self.emit_signal('MANAGER_INIT')
        logger.info('Started Worker: %s' % worker_key)


if __name__ == "__main__":
    import sys


    worker_key  = sys.argv[1]
    port = int(sys.argv[2])

    worker = Worker(port, worker_key)
    reactor.run()
