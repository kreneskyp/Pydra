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
from pydra.cluster.module import ModuleManager
from pydra.cluster.node import *
from pydra.cluster.tasks.task_manager import TaskManager

# init logging
import pydra_settings
from pydra.logging.logger import init_logging
logger = init_logging(pydra_settings.LOG_FILENAME_NODE)


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
            MasterConnectionManager,
            NodeZeroConfService
        ]

        ModuleManager.__init__(self)

        self.emit_signal('MANAGER_INIT')
        logger.info('Node - Started')



