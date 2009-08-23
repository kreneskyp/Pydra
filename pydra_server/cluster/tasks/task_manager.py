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
from pydra_server.cluster.tasks import packaging
from pydra_server.models import *
from pydra_server.util import graph

from twisted.internet import reactor

from threading import Lock

import tarfile, cStringIO, zlib
import os, shutil

import logging
logger = logging.getLogger('root')

# FIXME what we gonna do if a task is removed at the master side
# it seems that only an active pushing sync scheme will solve this problem

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
            self.task_history,
            self.task_history_detail
        ]

        self._listeners = {
            'MANAGER_INIT':self.init_task_cache,
            'TASK_RELOAD':self.read_task_package,
            'TASK_STARTED':self._task_started,
            'TASK_STARTED':self._task_stopped,
        }

        Module.__init__(self, manager)

        self.tasks_dir = pydraSettings.tasks_dir
        self.tasks_dir_internal = pydraSettings.tasks_dir_internal

        # full_task_key or pkg_name: pkg_object
        # preserved for both compatibility and efficiency
        self.registry = {} 
        self.package_dependency = graph.DirectedGraph()

        self._task_callbacks = {} # task_key : callback list

        self._lock = Lock()
        self._callback_lock = Lock()

        self.__initialized = False

        # in seconds; currently no way to customize this value
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
            task = self.registry[key, None].tasks[key]
            if task.form:
                t = loader.get_template('task_parameter_form.html')
                c = Context ({'form':task.form()})
                rendered_form = t.render(c)
            else:
                rendered_form = None

            message[key] = {'description':task.description ,'last_run':last_run, 'form':rendered_form}

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
        with self._lock:
            # read task_cache_internal (this is one-time job)
            files = os.listdir(self.tasks_dir_internal)
            for pkg_name in files:
                pkg_dir = os.path.join(self.tasks_dir_internal, pkg_name)
                if os.path.isdir(pkg_dir):
                    versions = os.listdir(pkg_dir)
                    if len(versions) != 1:
                        logger.error('Internal task cache is not clean!')
                    else:
                        v = versions[0]
                        full_pkg_dir = os.path.join(pkg_dir, v)
                        pkg = packaging.TaskPackage(pkg_name, full_pkg_dir)
                        if pkg.version <> v:
                            # verification
                            logger.warn('Invalid package %s:%s' % (pkg_name, v))
                        else:
                            self._add_package(pkg)

        # trigger the autodiscover procedure
        reactor.callLater(self.scan_interval, self.autodiscover)


    def autodiscover(self):
        """
        Periodically scan the task_cache folder.
        """
        old_packages = self.list_task_packages()

        files = os.listdir(self.tasks_dir)
        for filename in files:
            pkg_dir = os.path.join(self.tasks_dir, filename)
            if os.path.isdir(pkg_dir):
                pkg = self.read_task_package(filename)
                try:
                    old_packages.remove(pkg.name)
                except ValueError:
                    pass

        for pkg_name in old_packages:
            self.emit('TASK_REMOVED', pkg_name)

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

        return {
                'prev':paginated.has_previous(),
                'next':paginated.has_next(),
                'page':page,
                'instances':[instance for instance in paginated.object_list]
               }


    def task_history_detail(self, task_id):
        """
        Returns detailed history about a specific task_instance
        """

        try:
            task_instance = TaskInstance.objects.get(id=task_id)
        except TaskInstance.DoesNotExist:
            return None

        workunits = [workunit for workunit in task_instance.workunits.all() \
            .order_by('id')]
        task_key = task_instance.task_key
        task = self.registry[task_key, None].tasks[task_key]
        return {
                    'details':task_instance,
                    'name':task.__name__,
                    'description':task.description,
                    'workunits':workunits
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
            pkg = self.registry.get( (pkg_name, version), None)
            if pkg:
                logger.info('FOUNDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD')
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
            self.emit('TASK_OUTDATED', pkg_name)
            try:
                self._task_callbacks[pkg_name].append( (task_key, errcallback,
                            callback, callback_args, callback_kwargs) )
            except KeyError:
                self._task_callbacks[pkg_name]= [ (task_key, errcallback,
                        callback, callback_args, callback_kwargs) ]


    def active_sync(self, pkg_name, response=None, phase=1):
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

        @param pkg_name: the name of the task package to be updated
        @param response: the response received from the remote side
        @param phase: the phase in which invocation of this method is made
        @return a pair of values consisting of the request as a string, and
                a flag indicating whether it expect a remote response any more.
        """
        def on_remove_error(func, path, execinfo):
            logger.error('Error %s occurred while trying to remove %s' %
                    (execinfo, path))

        pkg = self.registry.get(pkg_name, None)
        if phase == 1:
            return pkg.version if pkg else None, True
        elif phase == 2:
            pkg_folder = self.get_package_location(pkg_name)
            if response:
                buf = zlib.decompress(response)
                in_file = cStringIO.StringIO(buf)
                tar = tarfile.open(mode='r', fileobj=in_file)

                # remove old package files
                if pkg:
                    for f in os.listdir(pkg_folder):
                        full_path = os.path.join(pkg_folder, f)
                        if os.path.isdir(full_path):
                            shutil.rmtree(full_path, onerror=on_remove_error)
                        else:
                            os.remove(full_path)
                else:
                    # create the folder
                    os.mkdir(pkg_folder)

                # extract the new package files
                tar.extractall(pkg_folder)
                tar.close()
                in_file.close()
            self.read_task_package(pkg_name)
            return None, False
            

    def passive_sync(self, pkg_name, request, phase=1):
        """
        Passively generates response to a remote sync request. The other side
        of the synchronization can make use this response to synchronize its
        package content with this package.

        This synchronization process may take one or more times of
        communication with the remote side. The 'phase' parameter, starting
        from 1, specifies the phase that the invocation of this method is in.

        Subclass implementators should guarantee that active_sync() and
        passive_sync() match.

        @param pkg_name: the name of the task package to be updated
        @param request: the request received from the remote side
        @param phase: the phase in which invocation of this method is made
        @return the response data
        """
        # always sync with the latest version
        pkg = self.registry.get( (pkg_name, None), None)
        if pkg:
            if pkg.version <> request:
                pkg_folder = self.get_package_location(pkg.name)
                out_file = cStringIO.StringIO()
                tar = tarfile.open(mode='w', fileobj=out_file)
                for f in os.listdir(pkg_folder):
                    tar.add(os.path.join(pkg_folder, f), f)
                tar.close()
                diff = out_file.getvalue()
                diff = zlib.compress(diff)
                out_file.close()
                return diff
            return ''
        else:
            # no such package
            return None


    def list_task_keys(self):
        return [k[0] for k in self.registry.keys() if k[0].find('.') != -1]
    

    def list_task_packages(self):
        return [k[0] for k in self.registry.keys() if k[0].find('.') == -1]


    def read_task_package(self, pkg_name):
        # this method is slow in finding updates of tasks
        with self._lock:
            pkg_dir = self.get_package_location(pkg_name)
            signal = None
            sha1_hash = packaging.compute_sha1_hash(pkg_dir)
            internal_folder = os.path.join(self.tasks_dir_internal,
                    pkg_name, sha1_hash)
            pkg = self.registry.get( (pkg_name, None), None)
            if not pkg or pkg.version <> sha1_hash:
                # copy this folder to tasks_dir_internal
                try:
                    shutil.copytree(pkg_dir, internal_folder)
                except OSError:
                    logger.warn('Package %s v%s already exists' % (pkg_name,
                                sha1_hash))

            pkg = packaging.TaskPackage(pkg_name, internal_folder)

            # find updates
            if (pkg.name, None) not in self.registry:
                signal = 'TASK_ADDED'
                updated = True
            elif pkg.version <> self.registry[pkg.name, None].version:
                signal = 'TASK_UPDATED'

            self._add_package(pkg)

            # make a copy in tasks_dir_internal


        if signal:
            # invoke attached task callbacks
            callbacks = self._task_callbacks.get(pkg_name, None)           
            module_path, cycle = self._compute_module_search_path(pkg_name)
            while callbacks:
                task_key, errcallback, callback, args, kwargs = callbacks.pop(0)
                if cycle:
                    errcallback(task_key, pkg.verison,
                            'Cycle detected in dependency')
                else:
                    callback(task_key, pkg.version, pkg.tasks[task_key],
                            module_path, *args, **kwargs)
            self.emit(signal, pkg_name)

        return pkg


    def get_task_package(self, task_key):
        return self.registry.get( (task_key, None), None)


    def get_package_location(self, pkg_name):
        return os.path.join(self.tasks_dir, pkg_name)

    
    def _add_package(self, pkg):
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
        # record a task usage
        pass


    def _task_stopped(self, task_key, version):
        # check if the task is in use, and if not, remove it
        pass

