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
import tarfile
import zlib

from pydra_settings import TASKS_DIR_INTERNAL, TASKS_SYNC_CACHE
from pydra.cluster.module import Module

class TaskSyncServer(Module):

    def __init__(self):

        self._remotes = [
            ('NODE', self.sync_task),
        ]


    def sync_task(self, pkg_name, version):
        """
        Passively generates response to a sync request. The other side
        of the synchronization can make use this response to synchronize its
        package content with this package.

        Subclass implementators should guarantee that active_sync() and
        passive_sync() match.

        This function will use a cache of the versions if available.

        @param pkg_name: the name of the task package to be updated
        @param version: version of the task to retrieve
        @return a string that is a tarziped TaskPackage directory, or None if
        the task could not be found
        """
        tarzip = '%s/%s_%s' % (TASKS_SYNC_CACHE, pkg_name, version)
        if TASKS_SYNC_CACHE and os.path.exists(tarzip):
            out_file = open(tarzip, 'w').read()
        else:
            str_file = cStringIO.StringIO()
            tar = tarfile.open(mode='w', fileobj=str_file)
            pkg_folder = '%s/%s/%s' % (TASKS_DIR_INTERNAL, pkg_name, version)
            for f in os.listdir(pkg_folder):
                tar.add(os.path.join(pkg_folder, f), f)
            tar.close()
            out_file = zlib.compress(str_file.getvalue())
            str_file.close()
            if TASKS_SYNC_CACHE:
                tar_file = open(tarzip, 'w')
                tar_file.write(out_file)
                tar_file.close()
        return out_file if out_file else None