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



import os, sys, inspect
import hashlib

from pydra.cluster.tasks.tasks import *

import logging
logger = logging.getLogger('root')

STATUS_NORMAL = 0
STATUS_OUTDATED = 1

class TaskPackage:
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
    Depends = <pkg_name1>, <pkg_name2>,    
    """
    
    def __init__(self, name, folder, version=None):
        """
        @param version - version to assign to this task.  If None will be
                        computed by hashing the contents of the directory.  This
                        Allows the TaskManager or other classes that already
                        know the version to skip the expensive SHA1 generation.
                        setting the version to an arbitrary value that is
                        incorrect will likely cause bad errors, be forewarned!
        """
        self.name = name
        self.dependency = [] # a list of depended package names

        self.status = STATUS_NORMAL

        self.tasks = {} # task_key: task_class
        self.version = version

        self._init(folder)


    def _init(self, pkg_folder):
        """
        Init this package by reading the folder passed in.  The directory name
        may be as the user defines it.  Or it may be a directory name that
        specifies the hash of the package.

        @param pkg_folder
        """
        if os.path.exists(pkg_folder):
            if not self.version:
                self.version = compute_sha1_hash(pkg_folder)
            
            logger.info('Loading Package: %s - %s' % (self.name, self.version))
            meta = _read_config(os.path.join(pkg_folder, 'META'))
            try:
                self.dependency = meta['Dependency'].split(', \t')
            except KeyError:
                pass

            # copied from task_manager.py
            sys.path.append(pkg_folder)
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

                                self.tasks['%s.%s.%s' % (self.name, module,
                                        task_key)] = task_class
                                logger.info('Loaded task: %s' % key)

                            except:
                                logger.error('ERROR Loading task: %s' % key)

            


def _read_config(meta_file_name):
    """
    Reads options from the META config file.
    """
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


def compute_sha1_hash(folder):
    """
    Computes the hash of all files in the task directory.
    """
    def hash_visitor(digester, dirname, names):
        for name in filter(lambda x: not x.endswith('.pyc'), names):
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

