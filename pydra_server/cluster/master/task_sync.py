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

from pydra_server.cluster.module import Module
from pydra_server.cluster.tasks.task_manager import TaskManager

class TaskSyncServer(Module):

    def __init__(self, manager):

        self._remotes = [
            ('NODE', self.sync_task),
        ]

        self._friends = {
            'task_manager' : TaskManager,
        }

        Module.__init__(self, manager)

    def sync_task(self, pkg_name, request, phase):
        """
        A remote method for TaskSyncClient's to sync task packages
        """
        resp = self.task_manager.passive_sync(pkg_name, request, phase)
        return pkg_name, resp, phase + 1

