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

from django.template import Context, loader

from pydra_server.cluster.tasks.tasks import *
from pydra_server.models import TaskInstance

import logging
logger = logging.getLogger('root')

""" TaskManager - Class that tracks and controls tasks
"""
class TaskManager():

    def __init__(self):
        self.registry = {}


    """ Registers a task making it available through the manager

    @param key: key for task
    @param task: task instance
    """
    def register(self, key, task):
        self.registry[key] = task


    """ deregisters a task, stopping it and removing it from the manager

    @param key: key for task
    """
    def deregister(self, key):
        # remove the task from the registry
        del self.registry[key]


    """ Iterates through a task and its children to build an array display information

    @param task: Task to process
    @param tasklist: Array to append data onto.  Uused for recursion.
    """
    def processTask(self, task, tasklist=None, parent=False):
        # initial call wont have an area yet
        if tasklist==None:
            tasklist = []

        #turn the task into a tuple
        processedTask = [task.__class__.__name__, parent, task.msg]

        #add that task to the list
        tasklist.append(processedTask)

        #add all children if the task is a container
        if isinstance(task,TaskContainer):
            for subtask in task.subtasks:
                self.processTask(subtask.task, tasklist, task.id)

        return tasklist


    """ Iterates through a task and its children to build an array of status information
    @param task: Task to process
    @param tasklist: Array to append data onto.  Uused for recursion.
    """
    def processTaskProgress(self, task, tasklist=None):
        # initial call wont have an area yet
        if tasklist==None:
            tasklist = []

        #turn the task into a tuple
        processedTask = {'id':task.id, 'status':task.status(), 'progress':task.progress(), 'msg':task.progressMessage()}

        #add that task to the list
        tasklist.append(processedTask)

        #add all children if the task is a container
        if isinstance(task,TaskContainer):
            for subtask in task.subtasks:
                self.processTaskProgress(subtask.task, tasklist)

        return tasklist


    """
    listTasks - builds a list of tasks
    @param keys: filters list to include only these tasks
    """
    def list_tasks(self, toplevel=True, keys=None):
        message = {}
        # show all tasks by default
        if keys == None:
            keys = self.registry.keys()

        for key in keys:
            try:
                last_run_instance = TaskInstance.objects.filter(task_key=key).exclude(completed=None).order_by('-completed').values_list('completed','task_key')[0]
                last_run = last_run_instance[0]
            #no instances
            except (KeyError, IndexError):
                last_run = None

            # render the form if the task has one
            if self.registry[key].form:
                t = loader.get_template('task_parameter_form.html')
                c = Context ({'form':self.registry[key].form()})
                rendered_form = t.render(c)
            else:
                rendered_form = None

            message[key] = {'description':self.registry[key].description ,'last_run':last_run, 'form':rendered_form}

        return message

    """
    builds a dictionary of progresses for tasks
    @param keys: filters list to include only these tasks
    """
    def progress(self, keys=None):
        message = {}

        # show all tasks by default
        if keys == None:
            keys = self.registry.keys()

        # store progress of each task in a dictionary
        for key in keys:
            progress = self.processTaskProgress(self.registry[key])
            message[key] = {
                'status':progress
            }

        return message


    """
    Auto-discover any tasks that are in the tasks directory
    """
    def autodiscover(self):
        import imp
        import os
        import inspect

        # Step 1: get all python files in the tasks directory
        files = os.listdir('./pydra_server/task_cache')

        # Step 2: iterate through all the python files importing each one and 
        #         and add it as an available Task
        for filename in files:
            if filename <> '__init__.py' and filename[-3:] == '.py':
                module = 'pydra_server.task_cache.%s' % filename[:-3]
                tasks = __import__(module, {}, {}, ['Task'])

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

                            self.register(task_key, task_class)
                            logger.info('Loaded task: %s' % key)
                        except:
                            logger.error('ERROR Loading task: %s' % key)



