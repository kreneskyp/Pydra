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

class TaskSyncModule(Module):

    _shared = [
        'workers',
    ]    

    def __init__(self):

        self._remotes = [
            ('REMOTE_WORKER', self.sync_task),
        ]

        self._friends = {
            'task_manager' : TaskManager,
        }

        Module.__init__(self, module)

    def sync_task(self, task_key, request, phase):
        resp = self.task_manager.passive_sync(task_key, request, phase)
        return task_key, resp, phase + 1

