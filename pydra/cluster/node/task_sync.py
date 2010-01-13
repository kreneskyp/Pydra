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
import cStringIO
import os
import shutil
import tarfile
import zlib

from pydra_settings import TASKS_DIR_INTERNAL
from pydra.cluster.module import Module

class TaskSyncClient(Module):

    _signals = [
        'TASK_RELOAD',
    ]

    _shared = [
        'master',
    ]

    def __init__(self):
        self._listeners = {
            'TASK_OUTDATED' : self.request_sync,
        }


    def request_sync(self, pkg_name, version):
        """
        Internal method to send sync requests to the TaskSyncServer.

        @param pkg_name - name of package to sync
        @param version - version of package to sync
        """
        deferred = self.master.remote.callRemote('sync_task', pkg_name, version)
        deferred.addCallback(self.receive_sync, pkg_name, version)


    def receive_sync(self, file, pkg_name, version):
        """
        Receives a TaskPackage as a tarzip'd file.  The file is unpacked into
        TASK_DIR_INTERNAL
        
        Subclass implementators should guarantee that active_sync() and
        passive_sync() match.

        @param pkg_name: the name of the task package to be updated
        @param version: version of the task to retrieve
        @param file: the tarziped TaskPackage directory
        """
        pkg_folder = '%s/%s/%s' % (TASKS_DIR_INTERNAL, pkg_name, version)
        buf = zlib.decompress(file)
        in_file = cStringIO.StringIO(buf)
        tar = tarfile.open(mode='r', fileobj=in_file)

        # delete existing folder if it exists
        if os.path.exists(pkg_folder):                    
            shutil.rmtree(pkg_folder)
        os.mkdir(pkg_folder)

        # extract the new package files
        tar.extractall(pkg_folder)
        tar.close()
        in_file.close()

        self.emit('TASK_RELOAD', pkg_name, version)
