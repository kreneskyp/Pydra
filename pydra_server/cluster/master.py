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

# ==========================================================
# Setup django environment 
# ==========================================================

import sys
import os

#python magic to add the current directory to the pythonpath
sys.path.append(os.getcwd())

#
if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
    os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

# ==========================================================
# Done setting up django environment
# ==========================================================


import os, sys
import time
import datetime

from threading import Lock

from zope.interface import implements
from twisted.cred import portal, checkers
from twisted.spread import pb
from twisted.application import service, internet
from twisted.internet import reactor, defer
from twisted.internet.error import AlreadyCalled
from twisted.web import server, resource
from twisted.cred import credentials
from django.utils import simplejson

from pydra_server.models import Node, TaskInstance
from pydra_server.cluster.constants import *
from pydra_server.cluster.tasks.task_manager import TaskManager
from pydra_server.cluster.auth.rsa_auth import RSAClient, load_crypto
from pydra_server.cluster.auth.worker_avatar import WorkerAvatar
from pydra_server.cluster.amf.interface import AMFInterface


class NodeClientFactory(pb.PBClientFactory):
    """
    Subclassing of PBClientFactory to add auto-reconnect via Master's reconnection code.
    This factory is specific to the master acting as a client of a Node.
    """

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



class Master(object):
    """
    Master is the server that controls the cluster.  There must be one and only one master
    per cluster.  It will direct and delegate work taking place on the Nodes and Workers
    """

    def __init__(self):
        print '[info] starting master'

        #locks
        self._lock = Lock()         #general lock, use when multiple shared resources are touched
        self._lock_queue = Lock()   #for access to _queue

        #load rsa crypto
        self.pub_key, self.priv_key = load_crypto('./master.key')
        self.rsa_client = RSAClient(self.priv_key, callback=self.init_node)

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

    def get_services(self):
        """
        Get the service objects used by twistd
        """
        # setup cluster connections
        realm = MasterRealm()
        realm.server = self

        # setup worker security - using this checker just because we need
        # _something_ that returns an avatarID.  Its extremely vulnerable
        # but thats ok because the real authentication takes place after
        # the worker has connected
        self.worker_checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
        p = portal.Portal(realm, [self.worker_checker])

        #setup AMF gateway security
        checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
        checker.addUser("controller", "1234")

        #setup controller connection via AMF gateway
        # Place the namespace mapping into a TwistedGateway:
        from pyamf.remoting.gateway.twisted import TwistedGateway
        from pyamf.remoting import gateway
        interface = AMFInterface(self, checker)
        gw = TwistedGateway({ 
                        "controller": interface,
                        }, authenticator=interface.auth)
        # Publish the PyAMF gateway at the root URL:
        root = resource.Resource()
        root.putChild("", gw)

        #setup services
        from twisted.internet.ssl import DefaultOpenSSLContextFactory
        try:
            context = DefaultOpenSSLContextFactory('ca-key.pem', 'ca-cert.pem')
        except:
            print '[ERROR] - Problem loading certificate required for ControllerInterface from ca-key.pem and ca-cert.pem.  Generate certificate with gen-cert.sh'
            sys.exit()

        controller_service = internet.SSLServer(18801, server.Site(root), contextFactory=context)
        worker_service = internet.TCPServer(18800, pb.PBServerFactory(p))

        return controller_service,  worker_service


    def load_nodes(self):
        """
        Load node configuration from the database
        """
        print '[info] loading nodes'
        nodes = Node.objects.all()
        node_dict = {}
        for node in nodes:
            node_dict[node.id] = node
        print '[info] %i nodes loaded' % len(nodes)
        return node_dict


    def connect(self):
        """
        Make connections to all Nodes that are not connected.  This method is a single control 
        for connecting to nodes.  individual nodes cannot be connected to.  This is to ensure that
        only one attempt at a time is ever made to connect to a node.
        """
        #lock for two reasons:
        #  1) connect() cannot be called more than once at a time
        #  2) if a node fails while connecting the reconnect call will block till 
        #     connections are finished
        with self._lock:
            self.connecting=True

            "Begin the connection process"
            connections = []
            self.attempts = []
            for id, node in self.nodes.items():
                #only connect to nodes that aren't connected yet
                if not node.ref:
                    factory = NodeClientFactory(node, self)
                    reactor.connectTCP(node.host, 11890, factory)

                    # SSH authentication is not currently supported with perspectiveBroker.
                    # For now we'll perform a key handshake within the info/init handshake that already
                    # occurs.  Prior to the handshake completing access will be limited to info which
                    # is informational only.
                    #
                    # Note: The first time connecting to the node will accept and register whatever
                    # key is passed to it.  This is a small trade of in temporary insecurity to simplify
                    # this can be avoided by manually generating and setting the keys
                    #
                    #credential = credentials.SSHPrivateKey('master', 'RSA', node.pub_key, '', '')
                    credential = credentials.UsernamePassword('master', '1234')

                    deferred = factory.login(credential, client=self)
                    connections.append(deferred)
                    self.attempts.append(node)

            defer.DeferredList(connections, consumeErrors=True).addCallbacks(
                self.nodes_connected, errbackArgs=("Failed to Connect"))

            # Release the connection flag.
            self.connecting=False


    def nodes_connected(self, results):
        """
        Called with the results of all connection attempts.  Store connections and retrieve info from node.
        The node will respond with info including how many workers it has.
        """
        # process each connected node
        failures = False

        for result, node in zip(results, self.attempts):

            #successes
            if result[0]:
                # save reference for remote calls
                node.ref = result[1]


                print '[info] node:%s:%s - connected' % (node.host, node.port)

                # Authenticate with the node
                pub_key = node.load_pub_key()
                pub_key_encrypt = pub_key.encrypt if pub_key else None
                self.rsa_client.auth(node.ref, pub_key_encrypt, node=node)

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


    def reconnect_nodes(self, reset_counter=False):
        """
        Called to signal that a reconnection attempt is needed for one or more nodes.  This is the single control
        for requested reconnection.  This single control is used to ensure at most 
        one request for reconnection is pending.
        """
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


    def init_node(self, node):
        """
        Start the initialization sequence with the node.  The first
        step is to query it for its information.
        """
        d = node.ref.callRemote('info')
        d.addCallback(self.add_node, node=node)


    def add_node(self, info, node):
        """
        Process Node information.  Most will just be stored for later use.  Info will include
        a list of workers.  The master will then connect to all Workers.
        """

        # save node's information in the database
        node.cores = info['cores']
        node.cpu_speed = info['cpu']
        node.memory = info['memory']
        node.save()

        #node key to be used by node and its workers
        node_key_str = '%s:%s' % (node.host, node.port)


        # if the node has never been 'seen' before it needs the Master's key
        # encrypt it and send it the key
        if not node.seen:
            print '[Info] Node has not been seen before, sending it the Master Pub Key'
            pub_key = self.pub_key
            #twisted.bannana doesnt handle large ints very well
            #we'll encode it with json and split it up into chunks
            from django.utils import simplejson
            import math
            dumped = simplejson.dumps(pub_key)
            chunk = 100
            split = [dumped[i*chunk:i*chunk+chunk] for i in range(int(math.ceil(len(dumped)/(chunk*1.0))))]
            pub_key = split

        else:
            pub_key = None


        # add all workers
        for i in range(node.cores):
            worker_key = '%s:%i' % (node_key_str, i)
            self.worker_checker.addUser(worker_key, '1234')


        # we have allowed access for all the workers, tell the node to init
        d = node.ref.callRemote('init', self.host, self.port, node_key_str, pub_key)
        d.addCallback(self.node_ready, node)


    def node_ready(self, result, node):
        """ 
        Called when a call to initialize a Node is successful
        """
        print '[Info] node:%s - ready' % node

        # if there is a public key from the node, unpack it and save it
        if result:
            dec = [self.priv_key.decrypt(chunk) for chunk in result]
            json_key = ''.join(dec)
            node.pub_key = json_key
            node.seen = True
            node.save()


    def worker_authenticated(self, worker_avatar):
        """
        Callback when a worker has been successfully authenticated
        """
        #request status to determine what this worker was doing
        deferred = worker_avatar.remote.callRemote('status')
        deferred.addCallback(self.add_worker, worker=worker_avatar, worker_key=worker_avatar.name)


    def add_worker(self, result, worker, worker_key):
        """
        Add a worker avatar as worker available to the cluster.  There are two possible scenarios:
        1) Only the worker was started/restarted, it is idle
        2) Only master was restarted.  Workers previous status must be reestablished

        The best way to determine the state of the worker is to ask it.  It will return its status
        plus any relevent information for reestablishing it's status
        """
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


    def remove_worker(self, worker_key):
        """
        Called when a worker disconnects
        """
        with self._lock:
            # if idle, just remove it.  no need to do anything else
            if worker_key in self._workers_idle:
                print '[info] worker:%s - removing worker from idle pool' % worker_key
                self._workers_idle.remove(worker_key)

            #worker was working on a task, need to clean it up
            else:
                removed_worker = self._workers_working[worker_key]

                #worker was working on a subtask, return unfinished work to main worker
                if removed_worker[3]:
                    print '[warning] %s failed during task, returning work unit' % worker_key
                    task_instance = TaskInstance.objects.get(id=removed_worker[0])
                    main_worker = self.workers[task_instance.worker]
                    if main_worker:
                        d = main_worker.remote.callRemote('return_work', removed_worker[3], removed_worker[4])
                        d.addCallback(self.return_work_success, worker_key)
                        d.addErrback(self.return_work_failed, worker_key)

                    else:
                        #if we don't have a main worker listed it probably already was disconnected
                        #just call successful to clean up the worker
                        self.return_work_success(None, worker_key)

                #worker was main worker for a task.  cancel the task and tell any
                #workers working on subtasks to stop.  Cannot recover from the 
                #main worker going down
                else:
                    #TODO
                    pass


    def return_work_success(self, results, worker_key):
        """
        Work was sucessful returned to the main worker
        """
        with self._lock:
            del self._workers_working[worker_key]


    def return_work_failed(self, results, worker_key):
        """
        A worker disconnected and the method call to return the work failed
        """
        pass


    def select_worker(self, task_instance_id, task_key, args={}, subtask_key=None, workunit_key=None):
        """
        Select a worker to use for running a task or subtask
        """
        #lock, selecting workers must be threadsafe
        with self._lock:
            if len(self._workers_idle):
                #move the first worker to the working state storing the task its working on
                worker_key = self._workers_idle.pop(0)
                self._workers_working[worker_key] = (task_instance_id, task_key, args, subtask_key, workunit_key)

                #return the worker object, not the key
                return self.workers[worker_key]
            else:
                return None


    def queue_task(self, task_key, args={}, subtask_key=None):
        """
        Queue a task to be run.  All task requests come through this method.  It saves their
        information in the database.  If the cluster has idle resources it will start the task
        immediately, otherwise it will queue the task until it is ready.
        """
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


    def cancel_task(self, task_id):
        """
        Cancel a task.  This function is used to cancel a task that was scheduled. 
        If the task is in the queue still, remove it.  If it is running then
        send signals to all workers assigned to it to stop work immediately.
        """
        task_instance = TaskInstance.objects.get(id=task_id)
        print '[info] Cancelling Task: %s' % task_id
        with self._lock_queue:
            if task_instance in self._queue:
                #was still in queue
                self._queue.remove(task_instance)
                print '[debug] Cancelling Task, was in queue: %s' % task_id
            else:
                print '[debug] Cancelling Task, is running: %s' % task_id
                #get all the workers to stop
                for worker_key, worker_task in self._workers_working.items():
                    if worker_task[0] == task_id:
                        worker = self.workers[worker_key]
                        print '[debug] signalling worker to stop: %s' % worker_key
                        worker.remote.callRemote('stop_task')

                self._running.remove(task_instance)

            task_instance.completion_type=-1
            task_instance.save()
            return 1


    def advance_queue(self):
        """
        Advances the queue.  If there is a task waiting it will be started, otherwise the cluster will idle.
        This should be called whenever a resource becomes available or a new task is queued
        """
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
                self._running.append(task_instance)

            else:
                # cluster does not have idle resources.
                # task will stay in the queue
                print '[debug] Task:%s:%s - no resources available, remaining in queue' % (task_instance.task_key, task_instance.subtask_key)
                return False


    def run_task(self, task_instance_id, task_key, args={}, subtask_key=None, workunit_key=None):
        """
        Run the task specified by the task_key.  This shouldn't be called directly.  Tasks should
        be queued with queue_task().  If the cluster has idle resources it will be run automatically

        This function is used internally by the cluster for parallel processing work requests.  Work
        requests are never queued.  If there is no resource available the main worker for the task
        should be informed and it can readjust its count of available resources.  Any type of resource
        sharing logic should be handled within select_worker() to keep the logic organized.
        """

        # get a worker for this task
        worker = self.select_worker(task_instance_id, task_key, args, subtask_key, workunit_key)
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


    def send_results(self, worker_key, results, workunit_key):
        """
        Called by workers when they have completed their task.

            Tasks runtime and log should be saved in the database
        """
        print '[debug] Worker:%s - sent results: %s' % (worker_key, results)
        with self._lock:
            task_instance_id, task_key, args, subtask_key, workunit_key = self._workers_working[worker_key]
            print '[info] Worker:%s - completed: %s:%s (%s)' % (worker_key, task_key, subtask_key, workunit_key)

            # release the worker back into the idle pool
            # this must be done before informing the 
            # main worker.  otherwise a new work request
            # can be made before the worker is released
            del self._workers_working[worker_key]
            self._workers_idle.append(worker_key)

            #if this was the root task for the job then save info.  Ignore the fact that the task might have
            #been canceled.  If its 100% complete, then mark it as such.
            if not subtask_key:
                with self._lock_queue:
                    task_instance = TaskInstance.objects.get(id=task_instance_id)
                    task_instance.completed = time.strftime('%Y-%m-%d %H:%M:%S')
                    task_instance.completion_type = 1
                    task_instance.save()

                    #remove task instance from running queue
                    try:
                        self._running.remove(task_instance)
                    except ValueError:
                        # was already removed by cancel
                        pass


            #check to make sure the task was still in the queue.  Its possible this call was made at the same
            # time a task was being canceled.  Only worry about sending the reults back to the Task Head
            # if the task is still running
            task_instance = TaskInstance.objects.get(id=task_instance_id)
            with self._lock_queue:
                if task_instance in self._running:
                    #if this was a subtask the main task needs the results and to be informed
                    task_instance = TaskInstance.objects.get(id=task_instance_id)
                    main_worker = self.workers[task_instance.worker]
                    print '[debug] Worker:%s - informed that subtask completed' % 'FOO'
                    main_worker.remote.callRemote('receive_results', results, subtask_key, workunit_key)

        #attempt to advance the queue
        self.advance_queue()


    def worker_stopped(self, worker_key):
        """
        Called by workers when they have stopped due to a cancel task request.
        """
        with self._lock:
            print '[info] Worker:%s - stopped' % worker_key

            # release the worker back into the idle pool
            # this must be done before informing the 
            # main worker.  otherwise a new work request
            # can be made before the worker is released
            del self._workers_working[worker_key]
            self._workers_idle.append(worker_key)

        #attempt to advance the queue
        self.advance_queue()


    def request_worker(self, workerAvatar, subtask_key, args, workunit_key):
        """
        Called by workers running a Parallel task.  This is a request
        for a worker in the cluster to process a workunit from a task
        """

        #get the task key and run the task.  The key is looked up
        #here so that a worker can only request a worker for the 
        #their current task.
        print self._workers_working
        worker = self._workers_working[workerAvatar.name]
        task_instance = TaskInstance.objects.get(id=worker[0])
        print '[debug] Worker:%s - request for worker: %s:%s' % (workerAvatar.name, subtask_key, args)

        # lock queue and check status of task to ensure no lost workers
        # due to a canceled task
        with self._lock_queue:
            if task_instance in self._running:
                self.run_task(worker[0], worker[1], args, subtask_key, workunit_key)

            else:
                print '[debug] Worker:%s - request for worker failed, task is not running' % (workerAvatar.name)



class MasterRealm:
    """
    Realm used by the Master server to assign avatars.
    """
    implements(portal.IRealm)
    def requestAvatar(self, avatarID, mind, *interfaces):
        assert pb.IPerspective in interfaces

        if avatarID == 'controller':
            avatar = ControllerAvatar(avatarID)
            avatar.server = self.server
            avatar.attached(mind)
            print '[info] controller:%s - connected' % avatarID

        else:
            key_split = avatarID.split(':')
            node = Node.objects.get(host=key_split[0], port=key_split[1])
            avatar = WorkerAvatar(avatarID, self.server, node)
            avatar.attached(mind)
            print '[info] worker:%s - connected' % avatarID

        return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)


#setup application used by twistd
master = Master()

application = service.Application("Pydra Master")

service1, service2 = master.get_services()
service1.setServiceParent(application)
service2.setServiceParent(application)
