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

from pydra.cluster.amf.interface import AMFInterface
from pydra.cluster.module import ModuleManager
from pydra.cluster.master import *
from pydra.cluster.master.task_sync import TaskSyncServer
from pydra.cluster.tasks.task_manager import TaskManager

import pydra_settings

# init logging
from pydra.logs.logger import init_logging
logger = init_logging(pydra_settings.LOG_FILENAME_MASTER)


class Master(ModuleManager):
    """
    Master is the server that controls the cluster.  There must be one and only one master
    per cluster (for now).  It will direct and delegate work taking place on the Nodes and Workers
    """

    def __init__(self):
        logger.info('====== starting master ======')

        """
        List of modules to load.  They will be loaded sequentially
        """
        self.modules = [
            AutoDiscoveryModule,
            NodeConnectionManager,
            TaskManager,
            TaskSyncServer,
            TaskScheduler,
            AMFInterface,
            NodeManager
        ]

        ModuleManager.__init__(self)

        self.emit_signal('MANAGER_INIT')




