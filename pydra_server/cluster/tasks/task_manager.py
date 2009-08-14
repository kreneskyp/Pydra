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

from django.core.paginator import Paginator, InvalidPage, EmptyPage
from django.template import Context, loader

from pydra_server.cluster.module import Module
from pydra_server.cluster.tasks.tasks import *
from pydra_server.models import *
from pydra_server.util import graph

from twisted.internet import reactor

import logging
logger = logging.getLogger('root')


class TaskManager(Module):
    """ 
    TaskManager - Class that tracks and controls tasks available to run on the
                  cluster.
    """

    _signals = [
        'TASK_ADDED',
        'TASK_UPDATED',
        'TASK_REMOVED',
        'TASK_INVALID',
    ]

    _shared = [
        'get_task',
    ]    

    def __init__(self, manager, scan_interval=10):

        self._interfaces = [
            self.list_tasks,
            self.task_history
        ]

        self._listeners = {
            'MANAGER_INIT':self.autodiscover,
        }

        Module.__init__(self, manager)

        self.task_packages = {} # pkg_name: pkg_object
        self.package_dependency = graph.DirectedGraph()

        # currently no way to customize this value
        self.scan_interval = scan_interval

    
    def register(self, key, task):
        """ Registers a task making it available through the manager

        @param key: key for task
        @param task: task instance
        """
        self.registry[key] = task


    def deregister(self, key):
        """ deregisters a task, stopping it and removing it from the manager

        @param key: key for task
        """
        # remove the task from the registry
        del self.registry[key]


    def processTask(self, task, tasklist=None, parent=False):
        """ Iterates through a task and its children to build an array display information

        @param task: Task to process
        @param tasklist: Array to append data onto.  Uused for recursion.
        """
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



    def processTaskProgress(self, task, tasklist=None):
        """ Iterates through a task and its children to build an array of status information
        @param task: Task to process
        @param tasklist: Array to append data onto.  Uused for recursion.
        """
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


    def list_tasks(self, toplevel=True, keys=None):
        """
        listTasks - builds a list of tasks
        @param keys: filters list to include only these tasks
        """
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

    
    def progress(self, keys=None):
        """
        builds a dictionary of progresses for tasks
        @param keys: filters list to include only these tasks
        """
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


    def discover(self):
        """
        A new discover method to periodically scan the task_cache folder.
        """
        task_dir = pydraSettings.tasks_dir

        # TODO emit TASK_UPDATED signal etc. when task updates are found
        files = os.listdir(task_dir)
        for filename in files:
            pkg_dir = os.path.join(task_dir, filename)
            if os.isdir(pkg_dir):
                self._read_task_package(pkg_dir)

        reactor.callLater(self.scan_interval, self.discover)

 
    def autodiscover(self):
        """
        Auto-discover any tasks that are in the tasks directory
        """
        import imp, os, sys, inspect

        for tasks_dir in pydraSettings.tasks_dir.split(','):

            # Step 1: get all python files in the tasks directory
            files = os.listdir(tasks_dir)
            sys.path.append(tasks_dir)

            # Step 2: iterate through all the python files importing each one and 
            #         and add it as an available Task
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

                                self.register(task_key, task_class)
                                logger.info('Loaded task: %s' % key)

                            except:
                                logger.error('ERROR Loading task: %s' % key)


    def task_history(self, key, page):

        instances = TaskInstance.objects.filter(task_key=key).order_by('-completed').order_by('-started')
        paginator = Paginator(instances, 10)

         # If page request (9999) is out of range, deliver last page of results.
        try:
            paginated = paginator.page(page)

        except (EmptyPage, InvalidPage):
            page = paginator.num_pages
            paginated = paginator.page(page)

        return {
                'prev':paginated.has_previous(),
                'next':paginated.has_next(),
                'page':page,
                'instances':[instance for instance in paginated.object_list]
               }



    def get_task(self, task_key):
        """
        task_key is referenced as 'package_name.task_name'

        @return task_class, pkg_version, additional_module_search_path
        """
        name = task_key.split('.')
        if len(name) == 2:
            pkg_name, task_name = name
            pkg = self.task_packages.get(pkg_name, None)
            if pkg:
                st, cycle = graph.dfs(self.package_dependency, pkg_name)
                if cycle:
                    raise RuntimeError('Cycle detected in task dependency')
                else:
                    required_pkgs = [self.task_packages[x].folder for x in \
                            st.keys() if  st[x] is not None]
                    return pkg.tasks[task_key], pkg.version, required_pkgs + \
                        [(x + '/lib') for x in required_pkgs]
        return None, None, None


    def active_sync(self, task_key, response=None, phase=1):
        """
        Generates an appropriate sync request.

        @param pkg_name: the name of the task package to be updated
        @param response: response received from the remote side
        @param phase: which step the sync process is in
        """
        pkg = self.task_packages.get(pkg_name, None)
        if not pkg:
            # the package does not exist yet
            pkg_folder = os.path.join(pydraSettings.tasks_dir, pkg_name)
            os.mkdir(pkg_folder)
            self._read_task_package(pkg_folder)

        pkg = self.task_packages.get(pkg_name, None)
            

    def passive_sync(self, pkg_name, request, phase=1):
        """
        Generates an appropriate sync response.

        @param pkg_name: the name of the task package to be updated
        @param response: response received from the remote side
        @param phase: which step the sync process is in
        """
        pkg = self.task_packages.get(pkg_name, None)
        if pkg:
            self.emit_signal('TASK_PASSIVE_SYNC_DATA',
                    pkg.passive_sync(request, phase))
        else:
            # no such task package
            self.emit_sigal('TASK_PASSIVE_SYNC_DATA', None)


    def _read_task_package(self, pkg_dir):
        pkg = packaging.TaskPackage(pkg_dir)
        for task_key in pkg.tasks.keys():
            self.tasks[task_key] = pkg
        self.task_packages[pkg.name] = pkg
        self.package_dependency.add_vertex(pkg.name)
        for dep in pkg.dependency:
            self.package_dependency.add_edge(pkg.name, dep)

