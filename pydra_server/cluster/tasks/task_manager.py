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
from __future__ import with_statement

from django.core.paginator import Paginator, InvalidPage, EmptyPage
from django.template import Context, loader

from pydra_server.cluster.module import Module
from pydra_server.cluster.tasks.tasks import *
from pydra_server.models import *
from pydra_server.util import graph

from twisted.internet import reactor

from threading import Lock

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
        'TASK_OUTDATED',
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
            'TASK_RELOAD':self.read_task_package,
        }

        Module.__init__(self, manager)

        # full_task_key or pkg_name: pkg_object
        # preserved for both compatibility and efficiency
        self.registry = {} 
        self.package_dependency = graph.DirectedGraph()

        self._task_callbacks = {} # task_key : callback list

        self._lock = Lock()

        # currently no way to customize this value
        self.scan_interval = scan_interval


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
            keys = self.list_task_keys()

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
            keys = self.list_task_keys()

        # store progress of each task in a dictionary
        for key in keys:
            progress = self.processTaskProgress(self.registry[key])
            message[key] = {
                'status':progress
            }

        return message


    def autodiscover(self):
        """
        Periodically scan the task_cache folder.
        """
        task_dir = pydraSettings.tasks_dir

        old_packages = self.list_task_packages()

        files = os.listdir(task_dir)
        for filename in files:
            pkg_dir = os.path.join(task_dir, filename)
            if os.isdir(pkg_dir):
                pkg = self.read_task_package(pkg_dir)
                old_packages.remove(pkg.name)

        for pkg_name in old_packages:
            self.emit_signal('TASK_REMOVED', pkg_name)

        reactor.callLater(self.scan_interval, self.autodiscover)


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



    def retrieve_task(self, task_key, version, callback, errcallback,
            *callback_args, **callback_kwargs):
        """
        task_key is referenced as 'package_name.task_name'

        @task_key: the task key
        @version: the version of the task
        @callback: callback to make after the latest task code is retrieved
        @errcallback: callback to make if task retrieval fails
        @callback_args: additional args for the callback
        @callback_kwargs: additional keyword args for the callback
        @return task_class, pkg_version, additional_module_search_path
        """
        pkg_name = task_key[:task_key.find('.')]
        needs_update = False
        with self._lock:
            pkg = self.registry.get(pkg_name, None)
            if pkg:
                pkg_status = pkg.status
                if pkg_status == packaging.STATUS_OUTDATED:
                    # package has already entered a sync process;
                    # append the callback
                    self._task_callbacks[pkg_name].append( (callback,
                                callback_args, callback_kwargs) )
                task_class = pkg.tasks.get(task_key, None)
                if task_class and (version is None or pkg.version == version):
                    module_search_path = [pkg.folder, pkg.folder + '/lib']
                    extra_path, cycle = self._compute_module_search_path(
                            pkg_name)
                    if cycle:
                        errcallback(task_key, verison,
                                'Cycle detected in dependency')
                    else:
                        callback(task_key, version, task_class,
                                module_search_path + extra_path, *callback_args,
                                **callback_kwargs)
                else:
                    # needs update
                    pkg.status = packaging.STATUS_OUTDATED
                    needs_update = True
            else:
                # no local package contains the specified task key, but this
                # does NOT mean it is an error - try synchronizing tasks first
                needs_update = True

        if needs_update:
            self.emit_signal('TASK_OUTDATED', pkg_name)
            try:
                self._task_callbacks[pkg_name].append( (task_key, callback,
                            callback_args, callback_kwargs) )
            except KeyError:
                self._task_callbacks[pkg_name]= [ (task_key, callback,
                            callback_args, callback_kwargs) ]


    def active_sync(self, pkg_name, response=None, phase=1):
        """
        Generates an appropriate sync request.

        @param pkg_name: the name of the task package to be updated
        @param response: response received from the remote side
        @param phase: which step the sync process is in
        """
        pkg = self.registry.get(pkg_name, None)
        if not pkg:
            # the package does not exist yet
            pkg_folder = os.path.join(pydraSettings.tasks_dir, pkg.name)
            os.mkdir(pkg_folder)
            self.read_task_package(pkg_folder)

        pkg = self.registry.get(pkg_name, None)
        return pkg.active_sync(response, phase)
            

    def passive_sync(self, pkg_name, request, phase=1):
        """
        Generates an appropriate sync response.

        @param pkg_name: the name of the task package to be updated
        @param response: response received from the remote side
        @param phase: which step the sync process is in
        """
        pkg = self.registry.get(pkg_name, None)
        if pkg:
            return pkg.passive_sync(request, phase)
        else:
            # no such task package
            return None


    def list_task_keys(self):
        return [k for k in self.registry.keys() if k.find('.') != -1]
    

    def list_task_packages(self):
        return [k for k in self.registry.keys() if k.find('.') == -1]


    def read_task_package(self, pkg_name):
        # this method is slow in finding updates of tasks
        pkg_dir = os.path.join(pydraSettings.tasks_dir, pkg_name)
        with self._lock:
            signals = []
            pkg = packaging.TaskPackage(pkg_dir)

            # find updates
            if pkg.name not in self.registry:
                signals.append('TASK_ADDED')
            else:
                if pkg.version <> self.registry[pkg.name].version:
                    signals.append('TASK_UPDATED')

            for task in pkg.tasks:
                key = '%s.%s' % (pkg.name, task.__class__.__name__)
                self.registry[key] = pkg
            self.registry[pkg.name] = pkg
            self.package_dependency.add_vertex(pkg.name)
            for dep in pkg.dependency:
                if dep not in self.registry.keys():
                    raise RuntimeError('Package %s has unresolved dependency
                            issues: %s' % (pkg_name, dep)
                self.package_dependency.add_edge(pkg.name, dep)

        for signal in signals:
            # invoke attached task callbacks
            callbacks = self._task_callbacks.get(pkg_name, None)           
            while callbacks:
                task_key, callback, args, kwargs = callbacks.pop()
                callback(task_key, self.registry[pkg_name).version, *args,
                        **kwargs)
            self.emit_signal(signal, pkg_name)
        return pkg


    def _compute_module_search_path(self, pkg_name):
        module_search_path = []
        st, cycle = graph.dfs(self.package_dependency, pkg_name)
        # computed packages on which this task depends
        required_pkgs = [self.registry[x].folder for x in \
                st.keys() if  st[x] is not None]
        module_search_path += required_pkgs
        module_search_path += [(x + '/lib') for x in required_pkgs]
        return module_search_path, cycle

