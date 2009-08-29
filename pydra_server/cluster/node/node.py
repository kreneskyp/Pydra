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


# ==========================================================
# Setup django environment 
# ==========================================================

import sys
import os

#python magic to add the current directory to the pythonpath
sys.path.append(os.getcwd())

if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
    os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
# ==========================================================
# Done setting up django environment
# ==========================================================

from twisted.internet import reactor
from twisted.application import service

from pydra_server.cluster.module import ModuleManager
from pydra_server.cluster.node import *
from pydra_server.cluster.node.task_sync import TaskSyncClient
from pydra_server.cluster.tasks.task_manager import TaskManager

# init logging
import settings
from pydra_server.logging.logger import init_logging
logger = init_logging(settings.LOG_FILENAME_NODE)


class NodeServer(ModuleManager):
    """
    Node - A Node manages a server in your cluster.  There is one instance of Node running per server.
        Node will spawn worker processes for each core available on your machine.  This allows some
        central control over what happens on the node.
    """
    def __init__(self):

        logger.info('===== Node - Starting =====')

        self.modules = [
            TaskManager,
            NodeInformation,
            WorkerManager,
            WorkerConnectionManager,
            MasterConnectionManager,
            TaskSyncClient,
            NodeZeroConfService
        ]

        ModuleManager.__init__(self)

        self.emit_signal('MANAGER_INIT')
        logger.info('Node - Started')


#root application object
application = service.Application('Pydra Node')

#create node server
node_server = NodeServer()

# attach service
for service in node_server.get_services():
    logger.info('Starting service: %s' % service)
    service.setServiceParent(application)
