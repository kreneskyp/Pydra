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

from module_avatar import ModuleAvatar
from rsa_auth import RSAAvatar
from Crypto.PublicKey import RSA

import logging
logger = logging.getLogger('root')

class WorkerAvatar(ModuleAvatar, RSAAvatar):
    """
    Class representing Workers for a Node.  This class encapsulates everything
    about a worker process and task.  It also contains all functions that the
    Worker is capable of calling
    """
    popen = None         # popen instance for controlling worker system process
    key = None           # Key that identifies task
    version = None       # version of task package containing task
    args = None          # Task arguments
    subtask_key = None   # key to subtask worker is assigned to
    local_subtask = None # key of subtask being run locally on the main worker
    workunit_key = None  # key to workunit worker is assigned to
    main_worker = None   # worker_key of mainworker for this task
    task_id = None       # unique identifier of Task this worker is assigned to
    run_task_deferred = None # defered set if run_task must be delayed
    remote = None        # remote referenceable object
    finished = False     # flag indicating this worker is finished and stopping

    def __init__(self, server, name):
        self.server = server
        self.name = name

        node_key = server.priv_key
        master_key = server.priv_key

        ModuleAvatar.__init__(self, server.manager._remotes['WORKER'])
        RSAAvatar.__init__(self, master_key, None, node_key, server.worker_authenticated, True)


    def attached(self, mind):
        """
        callback when avatar is connected
        """
        self.remote = mind


    def detached(self, mind):
        """
        callback when avatar is disconnected
        """
        logger.info('worker:%s - disconnected' % self.name)
        if self.authenticated:
            self.server.worker_disconnected(self)
        self.remote = None
