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

from threading import Lock, RLock
import os
import shutil
import time


from django.core.paginator import Paginator, InvalidPage, EmptyPage
from django.template import Context, loader
from twisted.internet import reactor

import pydra_settings
from pydra.cluster.module import Module
from pydra.cluster.tasks.tasks import *
from pydra.cluster.tasks import packaging
from pydra.models import *
from pydra.util import graph, makedirs

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

    lazy_init = False

    def __init__(self, scan_interval=20, lazy_init=False):
        """
        @param scan_interval - interval at which TASKS_DIR is scanned for
                                changes.  No scanning when set to None
        @param lazy_init - lazy init causes tasks to only be loaded when
                            requested.  Assumes scan_interval=None
        """
        
        self._interfaces = [
            self.list_tasks,
            self.task_history,
            self.task_history_detail,
            self.task_log,
        ]

        self._listeners = {
            'TASK_RELOAD':self.init_package,
            'TASK_STARTED':self._task_started,
            'TASK_STARTED':self._task_stopped,
        }
        
        if lazy_init:
            self.lazy_init = True
        else:
            self._listeners['MANAGER_INIT'] = self.init_task_cache

        self.tasks_dir = pydra_settings.TASKS_DIR
        self.tasks_dir_internal = pydra_settings.TASKS_DIR_INTERNAL

        makedirs(self.tasks_dir_internal)

        # full_task_key or pkg_name: pkg_object
        # preserved for both compatibility and efficiency
        self.registry = {}
        self.package_dependency = graph.DirectedGraph()

        self._task_callbacks = {} # task_key : callback list

        self._lock = RLock()
        self._callback_lock = Lock()

        self.__initialized = False

        # in seconds, None causes no updates    
        self.scan_interval = scan_interval


    def processTask(self, task, tasklist=None, parent=False):
        """ Iterates through a task and its children to build an array display
        information

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
        """ Iterates through a task and its children to build an array of
        status information
        @param task: Task to process
        @param tasklist: Array to append data onto.  Uused for recursion.
        """
        # initial call wont have an area yet
        if tasklist==None:
            tasklist = []

        #turn the task into a tuple
        processedTask = {'id':task.id, 'status':task.status(), \
                         'progress':task.progress(), \
        'msg':task.progressMessage()}

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
                last_run = time.mktime(last_run_instance[0].timetuple())
            #no instances
            except (KeyError, IndexError):
                last_run = None

            # render the form if the task has one
            task = self.registry[key, None].tasks[key]
            if task.form:
                t = loader.get_template('task_parameter_form.html')
                c = Context ({'form':task.form()})
                rendered_form = t.render(c)
            else:
                rendered_form = None

            message[key] = {'description':task.description ,
                            'last_run':last_run,
                            'form':rendered_form}

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
            progress = self.processTaskProgress(self.registry[key,
                        None].tasks[key])
            message[key] = {
                'status':progress
            }

        return message


    def init_task_cache(self):
        """
        initializes the cache of tasks.  This scans TASKS_DIR_INTERNAL, the
        already versioned tasks.  
        """
        # read task_cache_internal (this is one-time job)
        files = os.listdir(self.tasks_dir_internal)
        for pkg_name in files:
            self.init_package(pkg_name)

        # trigger the autodiscover procedure immediately
        if self.scan_interval:
            reactor.callLater(0, self.autodiscover)


    def init_package(self, pkg_name, version=None):
        """
        Attempts to load a single package into the registry.
        
        @param pkg_name - name of package to load into the registry
        @param version - version of package to load into the registry.  Defaults
                         to None, resulting in latest version.
        @param returns pkg if loaded, None otherwise
        """
        with self._lock:
            pkg_dir = os.path.join(self.tasks_dir_internal, pkg_name)
            if os.path.isdir(pkg_dir):
                versions = os.listdir(pkg_dir)
                if not versions:
                    raise TaskNotFoundException(pkg_name)

                elif version:
                    # load specified version if available
                    if not version in versions:
                        raise TaskNotFoundException(pkg_name)
                    v = version
                elif len(versions) != 1:
                    # load the newest version
                    logger.warn('Internal task cache contains more than one version of the task')
                    v = versions[0]
                    for dir in versions:
                        if os.path.getmtime('%s/%s' % (pkg_dir,dir)) > \
                            os.path.getmtime('%s/%s' % (pkg_dir,v)):
                                v = dir
                else:
                    v = versions[0]
                
                # load this version
                full_pkg_dir = os.path.join(pkg_dir, v)
                pkg = packaging.TaskPackage(pkg_name, full_pkg_dir, v)
                if pkg.version <> v:
                    # verification
                    logger.warn('Invalid package %s:%s' % (pkg_name, v))
                self._add_package(pkg)

                # invoke attached task callbacks
                callbacks = self._task_callbacks.get(pkg_name, None)           
                module_path, cycle = self._compute_module_search_path(pkg_name)
                while callbacks:
                    task_key, errcallback, callback, args, kw = callbacks.pop(0)
                    if cycle:
                        errcallback(task_key, pkg.verison,
                                'Cycle detected in dependency')
                    else:
                        callback(task_key, pkg.version, pkg.tasks[task_key],
                                module_path, *args, **kw)
                return pkg
        return None

    def autodiscover(self):
        """
        Periodically scan the task_cache folder.  This function is used for
        checking for new or updated tasks.  This function is CPU intensive as
        read_task_package() computes the hash of a directory.
        """
        old_packages = self.list_task_packages()

        files = os.listdir(self.tasks_dir)
        for filename in files:
            pkg_dir = os.path.join(self.tasks_dir, filename)
            if os.path.isdir(pkg_dir):
                self.read_task_package(filename)
                try:
                    old_packages.remove(filename)
                except ValueError:
                    pass

        for pkg_name in old_packages:
            self.emit('TASK_REMOVED', pkg_name)
        
        if self.scan_interval:
            reactor.callLater(self.scan_interval, self.autodiscover)


    def task_history(self, key, page):
        """
        Returns a paginated list of of times a task was run.
        """

        instances = TaskInstance.objects.filter(task_key=key) \
            .order_by('-completed').order_by('-started')
        paginator = Paginator(instances, 10)

         # If page request (9999) is out of range, deliver last page.
        try:
            paginated = paginator.page(page)

        except (EmptyPage, InvalidPage):
            page = paginator.num_pages
            paginated = paginator.page(page)

        instances = [i.json_safe() for i in paginated.object_list]

        return {
                'prev':paginated.has_previous(),
                'next':paginated.has_next(),
                'page':page,
                'instances':instances
               }


    def task_history_detail(self, task_id):
        """
        Returns detailed history about a specific task_instance
        """

        try:
            task_instance = TaskInstance.objects.get(id=task_id)
        except TaskInstance.DoesNotExist:
            return None

        workunits = [workunit.json_safe() for workunit in
                     task_instance.workunits.all().order_by('id')]
        task_key = task_instance.task_key
        task = self.registry[task_key, None].tasks[task_key]
        return {
                    'details':task_instance.json_safe(),
                    'name':task.__name__,
                    'description':task.description,
                    'workunits':workunits
               }

    def task_log(self, task_id, subtask=None, workunit_id=None):
        """ 
        Returns the logfile for the given task.
    
        @param task - id of task
        @param subtask - task path to subtask, default = None
        @param workunut - workunit key, default = None
        """
        from pydra.logs.logger import task_log_path

        if subtask:
            dir, logfile = task_log_path(task_id, subtask, workunit_id)
        else:
            dir, logfile = task_log_path(task_id)

        fp = open(logfile, 'r')
        log = fp.read()
        fp.close()
        return log
    
    def retrieve_task(self, task_key, version, callback, errcallback,
            *callback_args, **callback_kwargs):
        """
        Retrieves a task and calls callback passing the retrieved task.  If the
        requested version is not available, it will be retrieved first and this
        function will be called back again.
        
        @task_key: the task key, referenced as 'package_name.task_name'
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
            
            # get the task. if configured for lazy init, this class will only
            # attempt to load a task into the registry once it is requested.
            # subsequent requests will pull from the registry.
            pkg = self.registry.get( (pkg_name, version), None)
            if not pkg and self.lazy_init:
                logger.debug('Lazy Init: %s' % pkg_name)
                pkg = self.init_package(pkg_name, version)

            if pkg:
                pkg_status = pkg.status
                if pkg_status == packaging.STATUS_OUTDATED:
                    # package has already entered a sync process;
                    # append the callback
                    self._task_callbacks[pkg_name].append( (callback,
                                callback_args, callback_kwargs) )
                task_class = pkg.tasks.get(task_key, None)
                if task_class and (version is None or pkg.version == version):
                    module_path, cycle = self._compute_module_search_path(
                            pkg_name)
                    if cycle:
                        errcallback(task_key, verison,
                                'Cycle detected in dependency')
                    else:
                        callback(task_key, version, task_class, module_path,
                                *callback_args, **callback_kwargs)
                else:
                    # needs update
                    pkg.status = packaging.STATUS_OUTDATED
                    needs_update = True
            else:
                # no local package contains the task with the specified
                # version, but this does NOT mean it is an error - 
                # try synchronizing tasks first
                needs_update = True

        if needs_update:
            self.emit('TASK_OUTDATED', pkg_name, version)
            try:
                self._task_callbacks[pkg_name].append( (task_key, errcallback,
                            callback, callback_args, callback_kwargs) )
            except KeyError:
                self._task_callbacks[pkg_name]= [(task_key, errcallback,
                        callback, callback_args, callback_kwargs)]


    def list_task_keys(self):
        return [k[0] for k in self.registry.keys() if k[0].find('.') != -1]
    

    def list_task_packages(self):
        return [k[0] for k in self.registry.keys() if k[0].find('.') == -1]


    def read_task_package(self, pkg_name):
        """
        Reads the directory corresponding to a TaskPackage from TASKS_DIR and
        imports it into TASKS_DIR_INTERNAL.  Tasks have a hash computed that is
        used as the version.  Imported packages are also added to the registry
        and emit TASK_ADDED or TASK_UPDATED
        
        After updating or adding a new task, any callbacks pending for the
        tasks (ie. task waiting for sync) will be executed.
        
        This method is CPU intensive as all files within the package are SHA1
        hashed
        
        @param pkg_name - name of package, also the root directory.
        @returns TaskPackage if loaded, otherwise None
        """
        # this method is slow in finding updates of tasks
        with self._lock:
            pkg_dir = self.get_package_location(pkg_name)
            signal = None
            sha1_hash = packaging.compute_sha1_hash(pkg_dir)
            internal_folder = os.path.join(self.tasks_dir_internal,
                    pkg_name, sha1_hash)

            pkg = self.registry.get((pkg_name, None), None)
            if not pkg or pkg.version <> sha1_hash:
                # copy this folder to tasks_dir_internal
                try:
                    shutil.copytree(pkg_dir, internal_folder)
                except OSError:
                    # already in tree, just update the timestamp so it shows
                    # as the newest version
                    os.utime('%s' % (internal_folder), None)
                    logger.warn('Package %s v%s already exists' % (pkg_name,
                                sha1_hash))
            # find updates
            if (pkg_name, None) not in self.registry:
                signal = 'TASK_ADDED'
                updated = True
            elif sha1_hash <> self.registry[pkg.name, None].version:
                signal = 'TASK_UPDATED'

        if signal:
            pkg = self.init_package(pkg_name, sha1_hash)
            self.emit(signal, pkg_name)
            return pkg
        return None


    def get_task_package(self, task_key):
        return self.registry.get( (task_key, None), None)


    def get_package_location(self, pkg_name):
        return os.path.join(self.tasks_dir, pkg_name)

    
    def _add_package(self, pkg):
        """
        Adds a package to the registry, making it available for execution.
        Packages dependant on other packages that have not been loaded yet will
        cause an exception to be raised.
        
        @param pkg - TaskPackage to add
        """
        for dep in pkg.dependency:
            for key in self.registry.keys():
                if key[0] == dep:
                    break
            else:
                raise RuntimeError(
                        'Package %s has unresolved dependency issues: %s' %\
                        (pkg_name, dep))
            self.package_dependency.add_edge(pkg.name, dep)
        self.package_dependency.add_vertex(pkg.name)
        for key, task in pkg.tasks.iteritems():
            self.registry[key, pkg.version] = pkg
            self.registry[key, None] = pkg
        self.registry[pkg.name, pkg.version] = pkg

        # mark this package as the latest one
        self.registry[pkg.name, None] = pkg


    def _compute_module_search_path(self, pkg_name):
        """
        Creates a list of import paths that a package depends on.  These paths
        must be on the python path (sys.path) for this task package to be able
        to import its dependencies
        
        @param pkg_name - name of task package, also the root directory
        """
        pkg_location = self.get_package_location(pkg_name)
        module_search_path = [pkg_location, os.path.join(pkg_location,'lib')]
        st, cycle = graph.dfs(self.package_dependency, pkg_name)
        # computed packages on which this task depends
        required_pkgs = [self.get_package_location(x) for x in \
                st.keys() if  st[x] is not None]
        module_search_path += required_pkgs
        module_search_path += [os.path.join(x, 'lib') for x in required_pkgs]
        return module_search_path, cycle


    def _task_started(self, task_key, version):
        """
        Listener for task start.  Used for tracking Tasks that are currently
        running.
        """
        pass


    def _task_stopped(self, task_key, version):
        """
        Listener for task completion.  Used for deleting obsolete versions of
        Tasks that could not be deleted earlier due it being in use
        """
        pass

