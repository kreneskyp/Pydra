# Copyright 2009 Oregon State University
#
# This file is part of Pydra.
#
# Pydra is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Pydra is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Pydra.  If not, see <http://www.gnu.org/licenses/>.

"""
This module provides functionalities related to task packaging (ticket #74).

Each task, presented by a runnable item in the web UI, corresponds to a folder
in the task_cache directory. Such a folder is called a task package. A task
package is a self-contained unit, which includes the task definition itself
and other 3rd-party libraries that it depends on.

== Structure of the package folder ==
The structure of a task package typically looks like:

- lib
  + lib1_folder
  + lib2_folder
- task_def1.py
- task_def2.py
- META

where the "lib" folder contains depended modules, task_def1.py and task_def2.py
contains code defining tasks, and META is a text file describing the meta
information of this package.

There can be more than one tasks in a task package. To obtain a list of them,
the logic is to iterate over all the .py files at the root level of the package,
and then to find all classes derived from Task.

== Format of the META descriptor ==
Depends = <pkg_name1>, <pkg_name2>, ...

"""

import os, sys, inspect
import hashlib
import tarfile, cStringIO, zlib

from pydra_server.cluster.tasks.tasks import *

import logging
logger = logging.getLogger('root')

STATUS_NORMAL = 0
STATUS_OUTDATED = 1

class TaskPackage:
    
    def __init__(self, folder):
        self.folder = folder
        self.name = None
        self.dependency = [] # a list of depended package names

        self.status = STATUS_NORMAL

        self.tasks = {} # task_key: task_class
        self.version = None

        self._init(folder)


    def active_sync(self, response=None, phase=1):
        """
        Actively generates requests to update the local package to make
        it be the same as a remote package.

        This synchronization process may take one or more times of
        communication with the remote side. The 'phase' parameter, starting
        from 1, specifies the phase that the invocation of this method is in.
        The final step typically manipulates the local file system and makes
        the package content consistent with the remote package.

        Subclass implementators should guarantee that active_sync() and
        passive_sync() match.

        @param response: the response received from the remote side
        @param phase: the phase in which invocation of this method is made
        @return a pair of values consisting of the request as a string, and
                a flag indicating whether it expect a remote response any more.
        """
        def on_remove_error(func, path, execinfo):
            logger.error('Error %s occurred while trying to remove %s' %
                    (execinfo, path))

        if phase == 1:
            return self.version, True
        elif phase == 2:
            if response:
                buf = zlib.decompress(response)
                in_file = cStringIO.StringIO(buf)
                tar = tarfile.open(mode='r', fileobj=in_file)

                # remove old package files
                for f in os.listdir(self.folder):
                    full_path = os.path.join(self.folder, f)
                    if os.path.isdir(full_path):
                        shutil.rmtree(full_path, onerror=on_remove_error)
                    else:
                        os.remove(full_path)
                # extract the new package files
                tar.extractall(self.folder)
                tar.close()
                in_file.close()
                self._init(self.folder)
            return '', False


    def passive_sync(self, request, phase=1):
        """
        Passively generates response to a remote sync request. The other side
        of the synchronization can make use this response to synchronize its
        package content with this package.

        This synchronization process may take one or more times of
        communication with the remote side. The 'phase' parameter, starting
        from 1, specifies the phase that the invocation of this method is in.

        Subclass implementators should guarantee that active_sync() and
        passive_sync() match.

        @param request: the request received from the remote side
        @param phase: the phase in which invocation of this method is made
        @return the response data
        """
        if self.version <> snapshot:
            out_file = cStringIO.StringIO()
            tar = tarfile.open(mode='w', fileobj=out_file)
            for f in os.listdir(self.folder):
                tar.add(os.path.join(self.folder, f), f)
            tar.close()
            diff = out_file.getvalue()
            diff = zlib.compress(diff)
            out_file.close()
            return diff
        return ''


    def _init(self, pkg_folder):
        name = os.path.basename(pkg_folder)
        if name.find('.') != -1:
            raise RuntimeError('Package name should not contain dots (.)')

        meta = _read_config(os.path.join(pkg_folder, 'META'))
        try:
            self.dependency = meta['Dependency'].split(', \t')
        except KeyError:
            pass

        # copied from task_manager.py
        sys.path.append(pkg_folder) # FIXME importing logic seems incorrect
        files = os.listdir(pkg_folder)
        for filename in files:
            if filename <> '__init__.py' and filename[-3:] == '.py':
                module = filename[:-3]
                
                try:
                    tasks = __import__(module, {}, {}, ['Task'])
                except Exception, e:
                    logger.warn('Failed to load tasks from: %s (%s) - %s' % (module, filename, e))
                    continue

                # iterate through the objects in  the module to find Tasks
                # TODO replace this logic with code in Task that adds all tasks
                # to module.tasks when they are created.  This would be similar
                # to how django does this in db/base.py.  It's extremely complicated
                # and would take more time than is possible now.

                #class exclusions.  do not include any of these class
                class_exclusions = ('Task', 'ParallelTask', 'TaskContainer')

                for key, task_class in tasks.__dict__.items():

                    # Add any classes that a runnable task.
                    # TODO: filter out subtasks not marked as standalone
                    if inspect.isclass(task_class) and key not in class_exclusions and issubclass(task_class, (Task,)):

                        try:
                            #generate a unique key for this 
                            task_key = key

                            self.tasks[task_key] = task_class
                            logger.info('Loaded task: %s' % key)

                        except:
                            logger.error('ERROR Loading task: %s' % key)

        self.version = _compute_sha1_hash(pkg_folder)


def _read_config(meta_file_name):
    meta = {}
    try:
        mfile = open(meta_file_name, 'r')
        for line in mfile:
            line = line.strip()
            if not line.startswith('#'):
                # comments
                colon_pos = line.find('=')
                if colon_pos <> -1:
                    key = line[:colon_pos].rstrip()
                    value = line[colon_pos+1:].lstrip()
                    meta[key] = value
        mfile.close()
    except IOError:
        pass
    return meta


def _compute_sha1_hash(folder):
    def hash_visitor(digester, dirname, names):
        for name in names:
            f = open(os.path.join(dirname, name), 'r')
            digester.update(f.read())
            f.close()

    sha1_digester = hashlib.sha1()
    os.path.walk(folder, hash_visitor, sha1_digester)
    return sha1_digester.hexdigest()


class BsdiffTaskPackage(TaskPackage):

    def active_sync(self, response=None, phase=1):
        pass


    def passive_sync(self, request, phase=1):
        pass

