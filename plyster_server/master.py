"""
Controller.py
Starts and manages solvers in separate processes for parallel processing.
Provides an interface to the Flex UI.
"""
from __future__ import with_statement

import os, sys
from subprocess import Popen
from twisted.spread import pb
from twisted.internet import reactor, defer
from twisted.web import server, resource
from twisted.cred import credentials

from pyamf.remoting.gateway.twisted import TwistedGateway

class Master(pb.Referenceable):

    def __init__(self):
        self.workers = {}
        self.nodes = self.load_nodes()
        self.__workers_idle = []
        self.__workers_working = []
        self.connected = False
        self.connect()

    def load_nodes(self):
        return {('localhost',18801):{}}

    def failed(self, results, failureMessage="Call Failed"):
        for (success, returnValue), (address, port) in zip(results, self.nodes):
            if not success:
                raise Exception("address: %s port: %d %s" % (address, port, failureMessage))

    """
    Make initial connections to all Nodes
    """
    def connect(self):
        "Begin the connection process"
        connections = []
        for address, port in self.nodes:
            factory = pb.PBClientFactory()
            reactor.connectTCP(address, port, factory)
            deferred = factory.login(credentials.UsernamePassword("tester", "1234"), client=self)
            connections.append(deferred)

        defer.DeferredList(connections, consumeErrors=True).addCallbacks(
            self.store_node_connections, self.failed, errbackArgs=("Failed to Connect"))


    """
    Store connections and retrieve info from node.  The node will repsond with info including
    how many workers it has.  
    """
    def store_node_connections(self, results):
        # process each connected node
        for (success, node), (address, port) in zip(results, self.nodes):
            # save reference for remote calls
            self.nodes[address, port]['ref'] = node

            # Generate a node_key unique to this node instance, it will be used as
            # a base for the worker_key unique to the worker instance.  The
            # key will be used by its workers as identification and verification
            # that they really belong to the cluster.  This does not need to be 
            # extremely secure because workers only process data, they can't modify
            # anything anywhere else in the network.  The worst they should be able to
            # do is cause invalid results
            node_key = '%s:%s' % (address, port)

            #Initialize the node, this will result in it sending its info
            d = node.callRemote('initialize', node_key)
            d.addCallback(self.addNode, key=(address, port))


    """
    Process Node information.  Most will just be stored for later use.  Info will include
    a list of workers.  The master will then connect to all Workers
    """
    def addNode(self, info, key):
        #store the information with the node
        self.nodes[key]['info'] = info

        print self.nodes

    """
    Select a worker to use for running a task or subtask
    """
    def select_worker(self, task_key):
        #lock, selecting workers must be threadsafe
        with self.__lock:
            #move the first worker to the working state storing the task its working on
            worker_key = self.__workers_idle.pop(0)
            self.__workers_working[worker_key] = task_key

        #return the worker object, not the key
        return self.workers[worker_key]


    """
    Run the task specified by the task_key.  This should create
    a new instance of the specified task with a unique key
    """
    def run_task(self, task_key):
        #TODO create a better system for generating a key, probably just use django model ID
        task_instance_key = task_key
        worker = self.select_worker(task_instance_key)
        d = worker.callRemote('run_task', 'MasterTestTask')
        d.addCallback(self.my_print)

        reactor.callLater(3, worker.callRemote, 'status')


    """
    Called by workers when they have completed their task.

        Tasks runtime and log should be saved in the database
    """
    def send_results(self, worker_key, results):
        # release the worker back into the idle pool
        del self.__workers_working[worker_key]
        self.__workers_idle.append(worker_key)

        #if this was the root task for the job then save info

        #if this was a subtask the main task needs to be informed

        print results

    """
    Called by workers running a Parallel task.  This is a request
    for a worker in the cluster to process the args sent
    """
    def request_worker(self, task_key, args):
        pass

    """
    Store all successful connections made to Workers
    """
    def store_worker_connection(self, worker, key):
        self.workers[key] = {'ref':worker}
        self.__workers_idle.append(key)


class MasterRealm:
    implements(portal.IRealm)
    def requestAvatar(self, avatarID, mind, *interfaces):
        assert pb.IPerspective in interfaces
        avatar = WorkerAvatar(avatarID)
        avatar.server = self.server
        avatar.attached(mind)
        return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)

class WorkerAvatar(pb.Avatar):
    def __init__(self, name):
        self.name = name
        print '   worker connected'

    def attached(self, mind):
        self.remote = mind

    def detached(self, mind):
        self.remote = None

    """
    Called by workers when they have completed their task.

        Tasks runtime and log should be saved in the database
    """
    def perspective_send_results(self, results):
        self.server.send_results(worker_key, results)

    """
    Called by workers running a Parallel task.  This is a request
    for a worker in the cluster to process the args sent
    """
    def perspective_request_worker(self, task_key, args):
        pass

    def my_print(self, str):
        print str


if __name__ == "__main__":
    #master = MasterClient()
    #reactor.run()

    realm = MasterRealm()
    realm.server = Master()
    checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
    checker.addUser("tester", "1234")
    p = portal.Portal(realm, [checker])

    reactor.listenTCP(18800, pb.PBServerFactory(p))
    reactor.run()
