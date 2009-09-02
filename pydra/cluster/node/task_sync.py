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

class TaskSyncClient(Module):

    _signals = [
        'TASK_RELOAD',
    ]

    _shared = [
        'master',
    ]

    def __init__(self, manager):

        self._listeners = {
            'TASK_OUTDATED' : self.request_sync,
        }

        self._friends = {
            'task_manager' : TaskManager,
        }

        Module.__init__(self, manager)


    def request_sync(self, pkg_name):
        self._request_sync_internal( (pkg_name, None, 1) )


    def _request_sync_internal(self, response_tuple):
        """
        Internal method to send sync requests to the TaskSyncServer.

        @param response_tuple: a tuple of (pkg_name, response, phase)
        """
        # send the request to the master
        pkg_name, response, phase = response_tuple
        request = self.task_manager.active_sync(pkg_name, response, phase)
        if request[1]:
            # still expecting a remote answer; now send the req to the master
            deferred = self.master.remote.callRemote('sync_task', pkg_name, request[0],
                    phase)
            # using self as a callback
            deferred.addCallback(self._request_sync_internal)
        else:
            # the task has been successfully synchronized
            self.emit('TASK_RELOAD', pkg_name)

