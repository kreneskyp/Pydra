#!/usr/bin/env python

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

#
# Setup django environment when run from the commandline
#
if __name__ == '__main__':
    import sys
    import os

    #python magic to add the current directory to the pythonpath
    sys.path.append(os.getcwd())

    #
    if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
        os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'


import os, sys
import time
import datetime

from threading import Lock

from zope.interface import implements
from twisted.cred import portal, checkers
from twisted.spread import pb
from twisted.internet import reactor, defer
from twisted.internet.error import AlreadyCalled
from twisted.web import server, resource
from twisted.cred import credentials
from django.utils import simplejson

from pydra_server.models import Node, TaskInstance
from pydra_server.cluster.constants import *
from task_manager import TaskManager

"""
Subclassing of PBClientFactory to add auto-reconnect via Master's reconnection code.
This factory is specific to the master acting as a client of a Node.
"""
class NodeClientFactory(pb.PBClientFactory):
    node = None

    def __init__(self, node, master):
        self.node = node
        self.master = master
        pb.PBClientFactory.__init__(self)

    def clientConnectionLost(self, connector, reason):
        #lock - ensures that this blocks any connection attempts
        with self.master._lock:
            self.node.ref = None

        self.master.reconnect_nodes(True);
        pb.PBClientFactory.clientConnectionLost(self, connector, reason)


"""
Master is the server that controls the cluster.  There must be one and only one master
per cluster.  It will direct and delegate work taking place on the Nodes and Workers
"""
class Master(object):

    def __init__(self):
        print '[info] starting master'

        #locks
        self._lock = Lock()         #general lock, use when multiple shared resources are touched
        self._lock_queue = Lock()   #for access to _queue

        #load tasks queue
        self._running = list(TaskInstance.objects.running())
        self._running_workers = {}
        self._queue = list(TaskInstance.objects.queued())

        #cluster management
        self.workers = {}
        self.nodes = self.load_nodes()
        self._workers_idle = []
        self._workers_working = {}

        #connection management
        self.connecting = True
        self.reconnect_count = 0
        self.attempts = None
        self.reconnect_call_ID = None

        #load tasks that are cached locally
        #the master won't actually run the tasks unless there is also
        #a node running locally, but it will be used to inform the controller what is available
        self.task_manager = TaskManager()
        self.task_manager.autodiscover()
        self.available_tasks = self.task_manager.registry

        self.connect()

        self.host = 'localhost'
        self.port = 18800

    """
    Load node configuration from the database
    """
    def load_nodes(self):
        print '[info] loading nodes'
        nodes = Node.objects.all()
        node_dict = {}
        for node in nodes:
            node_dict[node.id] = node
        print '[info] %i nodes loaded' % len(nodes)
        return node_dict


    """
    Make connections to all Nodes that are not connected.  This method is a single control 
    for connecting to nodes.  individual nodes cannot be connected to.  This is to ensure that
    only one attempt at a time is ever made to connect to a node.
    """
    def connect(self):
        #lock for two reasons:
        #  1) connect() cannot be called more than once at a time
        #  2) if a node fails while connecting the reconnect call will block till 
        #     connections are finished
        with self._lock:
            self.connecting=True

            #clear the reconnect id, its already been called if it reached this far
            #if self.reconnect_call_ID:
            #    self.reconnect_call_ID = None

            "Begin the connection process"
            connections = []
            self.attempts = []
            for id, node in self.nodes.items():
                #only connect to nodes that aren't connected yet
                if not node.ref:
                    factory = NodeClientFactory(node, self)
                    reactor.connectTCP(node.host, 11890, factory)
                    deferred = factory.login(credentials.UsernamePassword("tester", "1234"), client=self)
                    connections.append(deferred)
                    self.attempts.append(node)

            defer.DeferredList(connections, consumeErrors=True).addCallbacks(
                self.nodes_connected, errbackArgs=("Failed to Connect"))

            # Release the connection flag.
            self.connecting=False

    """
    Called with the results of all connection attempts.  Store connections and retrieve info from node.
    The node will respond with info including how many workers it has.
    """
    def nodes_connected(self, results):
        # process each connected node
        failures = False

        for result, node in zip(results, self.attempts):

            #successes
            if result[0]:
                # save reference for remote calls
                node.ref = result[1]

                # Generate a node_key unique to this node instance, it will be used as
                # a base for the worker_key unique to the worker instance.  The
                # key will be used by its workers as identification and verification
                # that they really belong to the cluster.  This does not need to be 
                # extremely secure because workers only process data, they can't modify
                # anything anywhere else in the network.  The worst they should be able to
                # do is cause invalid results
                #node_key = '%s:%s' % (node.host, node.port)
                print '[info] node:%s:%s - connected' % (node.host, node.port)

                #Initialize the node, this will result in it sending its info
                d = node.ref.callRemote('info')
                d.addCallback(self.add_node, node=node)

            #failures
            else:
                print '[error] node:%s:%s - failed to connect' % (node.host, node.port)
                node.ref = None
                failures = True


        #single call to reconnect for all failures
        if failures:
            self.reconnect_nodes()

        else:
            self.reconnect_count = 0


    """
    Called to signal that a reconnection attempt is needed for one or more nodes.  This is the single control
    for requested reconnection.  This single control is used to ensure at most 
    one request for reconnection is pending.
    """
    def reconnect_nodes(self, reset_counter=False):
        #lock - Blocking here ensures that connect() cannot happen while requesting
        #       a reconnect.
        with self._lock:
            #reconnecting flag ensures that connect is only called a single time
            #it's possible that multiple nodes can have problems at the same time
            #reset_counter overrides this
            if not self.connecting or reset_counter:
                self.connecting = True

                #reset the counter, useful when a new failure occurs
                if reset_counter:
                    #cancel existing call if any
                    if self.reconnect_call_ID:
                        try:
                            self.reconnect_call_ID.cancel()

                        # There is a slight chance that this method can be called
                        # and receive the lock, after connect() has been called.
                        # in that case reconnect_call_ID will point to an already called
                        # item.  The error can just be ignored as the locking will ensure
                        # the call we are about to make does not start
                        # until the first one does.
                        except AlreadyCalled:
                            pass

                    self.reconnect_count = 0

                reconnect_delay = 5*pow(2, self.reconnect_count)
                #let increment grow exponentially to 5 minutes
                if self.reconnect_count < 6:
                    self.reconnect_count += 1 
                print '[debug] reconnecting in %i seconds' % reconnect_delay
                self.reconnect_call_ID = reactor.callLater(reconnect_delay, self.connect)

    """
    Process Node information.  Most will just be stored for later use.  Info will include
    a list of workers.  The master will then connect to all Workers
    """
    def add_node(self, info, node):

        # save node's information in the database
        node.cores = info['cores']
        node.cpu_speed = info['cpu']
        node.memory = info['memory']
        node.seen = True
        node.save()

        #add the Node's workers to the checker so they are allowed to connect
        node_key_str = '%s:%s' % (node.host, node.port)
        for i in range(info['cores']):
            self.checker.addUser('%s:%i' % (node_key_str, i), "1234")

        # we have allowed access for all the workers, tell the node to init
        d = node.ref.callRemote('init', self.host, self.port, node_key_str)
        d.addCallback(self.node_ready, node)

        #TODO Remove
        #reactor.callLater(10, self.queue_task, 'TestTask');
        #reactor.callLater(3, self.run_task, 'TestParallelTask');

    """ 
    Called when a call to initialize a Node is successful
    """
    def node_ready(self, result, node):
        print '[Info] node:%s - ready' % node


    """
    Add a worker avatar as worker available to the cluster.  There are two possible scenarios:
       1) Only the worker was started/restarted, it is idle
       2) Only master was restarted.  Workers previous status must be reestablished

       The best way to determine the state of the worker is to ask it.  It will return its status
       plus any relevent information for reestablishing it's status
    """
    def add_worker(self, result, worker, worker_key):
                # worker is working and it was the master for its task
                if result[0] == WORKER_STATUS_WORKING:
                    print '[info] worker:%s - is still working' % worker_key
                    #record what the worker is working on
                    #self._workers_working[worker_key] = task_key

                # worker is finished with a task
                elif result[0] == WORKER_STATUS_FINISHED:
                    print '[info] worker:%s - was finished, requesting results' % worker_key
                    #record what the worker is working on
                    #self._workers_working[worker_key] = task_key

                    #check if the Worker acting as master for this task is ready
                    if (True):
                        #TODO
                        pass

                    #else not ready to send the results
                    else:
                        #TODO
                        pass

                #otherwise its idle
                else:
                    with self._lock:
                        self.workers[worker_key] = worker
                        # worker shouldn't already be in the idle queue but check anyway
                        if not worker_key in self._workers_idle:
                            self._workers_idle.append(worker_key)
                            print '[info] worker:%s - added to idle workers' % worker_key

    """
    Called when a worker disconnects
    """
    def remove_worker(self, worker_key):
        with self._lock:
            # if idle, just remove it.  no need to do anything else
            if worker_key in self._workers_idle:
                print '[info] worker:%s - removing worker from idle pool' % worker_key
                self._workers_idle.remove(worker_key)

            #worker was working on a task, need to clean it up
            else:
                #worker was working on a subtask, return unfinished work to main worker
                if self._workers_working[worker_key][3]:
                    #TODO
                    pass

                #worker was main worker for a task.  cancel the task and tell any
                #workers working on subtasks to stop.  Cannot recover from the 
                #main worker going down
                else:
                    #TODO
                    pass

    """
    Select a worker to use for running a task or subtask
    """
    def select_worker(self, task_instance_id, task_key, args={}, subtask_key=None):
        #lock, selecting workers must be threadsafe
        with self._lock:
            if len(self._workers_idle):
                #move the first worker to the working state storing the task its working on
                worker_key = self._workers_idle.pop(0)
                self._workers_working[worker_key] = (task_instance_id, task_key, args, subtask_key)

                #return the worker object, not the key
                return self.workers[worker_key]
            else:
                return None


    """
    Queue a task to be run.  All task requests come through this method.  It saves their
    information in the database.  If the cluster has idle resources it will start the task
    immediately, otherwise it will queue the task until it is ready.
    """
    def queue_task(self, task_key, args={}, subtask_key=None):
        print '[info] Task:%s:%s - Queued' % (task_key, subtask_key)

        #create a TaskInstance instance and save it
        task_instance = TaskInstance()
        task_instance.task_key = task_key
        task_instance.subtask_key = subtask_key
        task_instance.args = simplejson.dumps(args)
        task_instance.save()

        #queue the task and signal attempt to start it
        with self._lock_queue:
            self._queue.append(task_instance)
        self.advance_queue()

        return task_instance


    """
    Advances the queue.  If there is a task waiting it will be started, otherwise the cluster will idle.
    This should be called whenever a resource becomes available or a new task is queued
    """
    def advance_queue(self):
        print '[debug] advancing queue: %s' % self._queue
        with self._lock_queue:
            try:
                task_instance = self._queue[0]

            except IndexError:
                #if there was nothing in the queue then fail silently
                print '[debug] No tasks in queue, idling'
                return False

            if self.run_task(task_instance.id, task_instance.task_key, simplejson.loads(task_instance.args), task_instance.subtask_key):
                #task started, update its info and remove it from the queue
                print '[info] Task:%s:%s - starting' % (task_instance.task_key, task_instance.subtask_key)
                task_instance.started = time.strftime('%Y-%m-%d %H:%M:%S')
                task_instance.save()
                del self._queue[0]

                self._running.append[task_instance]

            else:
                # cluster does not have idle resources.
                # task will stay in the queue
                print '[debug] Task:%s:%s - no resources available, remaining in queue' % (task_instance.task_key, task_instance.subtask_key)
                return False


    """
    Run the task specified by the task_key.  This shouldn't be called directly.  Tasks should
    be queued with queue_task().  If the cluster has idle resources it will be run automatically

    This function is used internally by the cluster for parallel processing work requests.  Work
    requests are never queued.  If there is no resource available the main worker for the task
    should be informed and it can readjust its count of available resources.  Any type of resource
    sharing logic should be handled within select_worker() to keep the logic organized.
    """
    def run_task(self, task_instance_id, task_key, args={}, subtask_key=None, workunit_key=None):

        # get a worker for this task
        worker = self.select_worker(task_instance_id, task_key, args, subtask_key)

        # determine how many workers are available for this task
        available_workers = len(self._workers_idle)+1

        if worker:
            print '[debug] Worker:%s - Assigned to task: %s:%s %s' % (worker.name, task_key, subtask_key, args)
            d = worker.remote.callRemote('run_task', task_key, args, subtask_key, workunit_key, available_workers)
            d.addCallback(self.run_task_successful, worker, task_instance_id, subtask_key)
            return 1

        # no worker was available
        # just return 0 (false), the calling function will decided what to do,
        # depending what called run_task, different error handling will apply
        else:
            print '[warning] No worker available'
            return 0

    def run_task_successful(self, results, worker, task_instance_id, subtask_key=None):

        #save the history of what workers work on what task/subtask
        #its needed for tracking finished work in ParallelTasks and will aide in Fault recovery
        #it might also be useful for analysis purposes if one node is faulty
        if subtask_key:
            #TODO, update model and record what workers worked on what subtasks
            pass

        else:
            task_instance = TaskInstance.objects.get(id=task_instance_id)
            task_instance.worker = worker.name
            task_instance.save()

    """
    Called by workers when they have completed their task.

        Tasks runtime and log should be saved in the database
    """
    def send_results(self, worker_key, results, workunit_key):
        print '[debug] Worker:%s - sent results: %s' % (worker_key, results)
        with self._lock:
            task_instance_id, task_key, args, subtask_key = self._workers_working[worker_key]
            print '[info] Worker:%s - completed: %s:%s (%s)' % (worker_key, task_key, subtask_key, workunit_key)

            # release the worker back into the idle pool
            # this must be done before informing the 
            # main worker.  otherwise a new work request
            # can be made before the worker is released
            del self._workers_working[worker_key]
            self._workers_idle.append(worker_key)


        #if this was the root task for the job then save info
        if not subtask_key:
            task_instance = TaskInstance.objects.get(id=task_instance_id)
            task_instance.completed = time.strftime('%Y-%m-%d %H:%M:%S')
            task_instance.save()


        #if this was a subtask the main task needs the results and to be informed
        else:
            task_instance = TaskInstance.objects.get(id=task_instance_id)
            main_worker = self.workers[task_instance.worker]
            print '[debug] Worker:%s - informed that subtask completed' % 'FOO'
            main_worker.remote.callRemote('receive_results', results, subtask_key, workunit_key)

        #attempt to advance the queue
        self.advance_queue()


    """
    Called by workers running a Parallel task.  This is a request
    for a worker in the cluster to process a workunit from a task
    """
    def request_worker(self, workerAvatar, subtask_key, args, workunit_key):
        #get the task key and run the task.  The key is looked up
        #here so that a worker can only request a worker for the 
        #their current task.
        print self._workers_working
        worker = self._workers_working[workerAvatar.name]
        print '[debug] Worker:%s - request for worker: %s:%s' % (workerAvatar.name, subtask_key, args)
        self.run_task(worker[0], worker[1], args, subtask_key, workunit_key)

"""
Realm used by the Master server to assign avatars.
"""
class MasterRealm:
    implements(portal.IRealm)
    def requestAvatar(self, avatarID, mind, *interfaces):
        assert pb.IPerspective in interfaces

        if avatarID == 'controller':
            avatar = ControllerAvatar(avatarID)
            avatar.server = self.server
            avatar.attached(mind)
            print '[info] controller:%s - connected' % avatarID

        else:
            avatar = WorkerAvatar(avatarID)
            avatar.server = self.server
            avatar.attached(mind)
            print '[info] worker:%s - connected' % avatarID

            #request status to determine what this worker was doing
            deferred = avatar.remote.callRemote('status')
            deferred.addCallback(self.server.add_worker, worker=avatar, worker_key=avatarID)

        return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)

"""
Avatar used by Workers connecting to the Master.
"""
class WorkerAvatar(pb.Avatar):
    def __init__(self, name):
        self.name = name

    def attached(self, mind):
        self.remote = mind

    def detached(self, mind):
        print '[info] worker:%s - disconnected' % self.name
        self.server.remove_worker(self.name)
        self.remote = None

    """
    Called by workers when they have completed their task and need to report the results.
       * Tasks runtime and log should be saved in the database
    """
    def perspective_send_results(self, results, workunit_key):
        return self.server.send_results(self.name, results, workunit_key)

    """
    Called by workers running a Parallel task.  This is a request
    for a worker in the cluster to process the args sent
    """
    def perspective_request_worker(self, subtask_key, args, workunit_key):
        return self.server.request_worker(self, subtask_key, args, workunit_key)


"""
Avatar used by Controllers connected to the Master
"""
class ControllerAvatar(pb.Avatar):
    def __init__(self, name):
        self.name = name

    def attached(self, mind):
        self.remote = mind

    def detached(self, mind):
        self.remote = None

    """
    Called when the controller wants an update of node statuses
    """
    def perspective_node_statuses(self):
        return self.server.node_statuses()

    """
    Called to start a task
    """
    def perspective_run_task(self, subtask_key, args):
        return self.server.request_worker(self, subtask_key, args)

    """
    Called to stop a task
    """
    def perspective_stop_task(self, task_instance_id, args):
        return self.server.request_worker(self, subtask_key, args)


"""
Class used to authenticate the request.  This is a hack of a class but
unfortunately required because there does not appear to be a better way
to block until a deferred completes
"""
class AMFAuthenticator(object):
    def __init__(self, checker):
        self.result = False
        self.checker = checker

    def auth_success(self, result):
        self.result = True
        self.auth = True

    def auth_failure(self, result):
        print '[error] Unauthorized attempt to use service'
        self.result = True
        self.auth = False

    def auth(self, user, password):
        from twisted.cred.credentials import UsernamePassword
        credentials = UsernamePassword(user, password)
        avatarId = self.checker.requestAvatarId(credentials)
        avatarId.addCallback(self.auth_success)
        avatarId.addErrback(self.auth_failure)

        # block for 5 seconds or until a result happens
        # in most cases a result should happen very quickly
        for i in range(25):
            if self.result:
                break
            time.sleep(.2)

        return self.auth

"""
Interface for Controller.  This exposes functions to a controller.
"""
class AMFInterface(pb.Root):

    def __init__(self, master, checker):
        self.master = master
        self.checker = checker

    def auth(self, user, password):
        authenticator = AMFAuthenticator(checker)
        return authenticator.auth(user, password)

    def is_alive(self, _):
        print '[debug] is alive'
        return 1

    def node_status(self, _):
        node_status = {}
        worker_list = self.master.workers
        #iterate through all the nodes adding their status
        for key, node in self.master.nodes.items():
            worker_status = {}
            if node.cores:
                #iterate through all the workers adding their status as well
                #also check for a worker whose should be running but is not connected
                for i in range(node.cores):
                    w_key = '%s:%s:%i' % (node.host, node.port, i)
                    html_key = '%s_%i' % (node.id, i)
                    if w_key in self.master._workers_idle:
                        worker_status[html_key] = (1,-1,-1)
                    elif w_key in self.master._workers_working:
                        task, subtask = self.master._workers_working[w_key]
                        worker_status[html_key] = (1,task,subtask)
                    else:
                        worker_status[html_key] = -1

            else:
                worker_status=-1

            node_status[key] = {'status':node.status(),
                                'workers':worker_status
                               }
        return node_status

    def list_tasks(self, _):
        return self.master.task_manager.list_tasks()

    def list_queue(self, _):
        return self.master._queue

    def run_task(self, _, key):
        return self.master.queue_task(key)

    def task_status(self, _):
        return self.master.task_manager.task_status()



if __name__ == "__main__":
    # setup cluster connections
    realm = MasterRealm()
    realm.server = Master()
    checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
    realm.server.checker = checker
    checker.addUser("controller", "1234")
    p = portal.Portal(realm, [checker])
    reactor.listenTCP(18800, pb.PBServerFactory(p))

    #setup controller connection via AMF gateway
    # Place the namespace mapping into a TwistedGateway:
    from pyamf.remoting.gateway.twisted import TwistedGateway
    from pyamf.remoting import gateway
    interface = AMFInterface(realm.server, checker)
    gw = TwistedGateway({ 
                    "controller": interface,
                    }, authenticator=interface.auth)
    # Publish the PyAMF gateway at the root URL:
    root = resource.Resource()
    root.putChild("", gw)
    # Tell the twisted reactor to listen:
    reactor.listenTCP(18801, server.Site(root))

    #sr = gateway.ServiceRequest(None, gw.services['controller'], None)
    #gw.authenticateRequest(sr, 'u', 'p')

    #start the server
    reactor.run()
