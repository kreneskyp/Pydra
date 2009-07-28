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
"""

import hashlib
import tarfile


def read_package(pkg_folder):
    """
    Returns a dict containing mappings from task keys to task classes.
    """
    tasks = {}

    # copied from task_manager.py
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

                        tasks[task_key] = task_class
                        logger.info('Loaded task: %s' % key)

                    except:
                        logger.error('ERROR Loading task: %s' % key)

    return tasks


def sync_package(old_p, new_p):
    pass
    

def package_hash(task_folder):
    pass


def archive_package(pkg_folder, out_file):
    """
    Archives a task package in a tar.gz file so that it can be transferred.
    """
    tar = tarfile.open(out_file, 'w:bz2')
    tar.add(pkg_folder)
    tar.close()


def get_python_path(task_folder):
    """
    Returns the path that needs to be appended to the module search path.
    """
    return [task_folder, task_folder + '/lib']

