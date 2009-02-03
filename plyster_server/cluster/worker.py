#! /usr/bin/python
from __future__ import with_statement

from twisted.spread import pb
from twisted.internet import reactor
from task_manager import TaskManager
from twisted.cred import credentials
import os, sys
from twisted.internet.protocol import ReconnectingClientFactory
from threading import Lock



class ReconnectingPBClientFactory(pb.PBClientFactory):
    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason
        #ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason
        #ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)




"""
Worker - The Worker is the workhorse of the cluster.  It sits and waits for Tasks and SubTasks to be executed
         Each Task will be run on a single Worker.  If the Task or any of its subtasks are a ParallelTask 
         the first worker will make requests for work to be distributed to other Nodes
"""
class Worker(pb.Referenceable):

    def __init__(self, master_host, master_port, node_key, worker_key):
        self.id = id
        self.__task = None
        self.__lock = Lock()
        self.master = None
        self.master_host = master_host
        self.master_port = master_port
        self.node_key = node_key
        self.worker_key = worker_key

        #load tasks that are cached locally
        self.task_manager = TaskManager()
        self.task_manager.autodiscover()
        self.available_tasks = self.task_manager.registry

        print '===== STARTED Worker ===='
        self.connect()

    """
    Make initial connections to all Nodes
    """
    def connect(self):
        "Begin the connection process"
        factory = ReconnectingPBClientFactory()
        reactor.connectTCP(self.master_host, self.master_port, factory)
        deferred = factory.login(credentials.UsernamePassword(self.worker_key, "1234"), client=self)
        deferred.addCallbacks(self.connected, self.connect_failed, errbackArgs=("Failed to Connect"))

    """
    Callback called when connection to master is made
    """
    def connected(self, result):
        self.master = result

    """
    Callback called when conenction to master fails
    """
    def connect_failed(self, result, *arg, **kw):
        print result

    """
     Runs a task on this worker
    """
    def run_task(self, key, args={}, subtask_key=None, available_workers=1):
        #Check to ensure this worker is not already busy.
        # The Master should catch this but lets be defensive.
        with self.__lock:
            if self.__task:
                return "FAILURE THIS WORKER IS ALREADY RUNNING A TASK"
            self.__task = key

        self.available_workers = available_workers

        print 'Starting task: %s' % key

        #create an instance of the requested task
        self.__task_instance = object.__new__(self.available_tasks[key])
        self.__task_instance.__init__()
        self.__task_instance.parent = self

        return self.__task_instance.start(args, subtask_key, self.work_complete)

    """
    Return the status of the current task if running, else None
    """
    def status(self):
        if self.__task:
            return self.available_tasks[self.__task].status()
        return None


    """
    Callback that is called when a job is run in non_blocking mode.  This function
    saves the results until retrieved by get_results()
    """
    def work_complete(self, results):
        _results = results
        self.__task = None
        self.master.callRemote("send_results", results)

    """
    Requests a work unit be handled by another worker in the cluster
    """
    def request_worker(self, subtask_key, args):
        deferred = self.master.request_worker(self.__task, subtask_key, args)

    """
    Recursive function so tasks can find this worker
    """
    def get_worker(self):
        return self


    # returns the status of this node
    def remote_status(self):
        return self.status()

    # returns the list of available tasks
    def remote_task_list(self):
        return self.available_tasks.keys()

    # run a task
    def remote_run_task(self, key, args={}, subtask_key=None, available_workers=1):
        return self.run_task(key, args, subtask_key, available_workers)



if __name__ == "__main__":
    master_host = sys.argv[1]
    master_port = int(sys.argv[2])
    node_key    = sys.argv[3]
    worker_key  = sys.argv[4]

    '''realm = ClusterRealm()
    realm.server = Worker('%s:%s' % (host,port))
    checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
    checker.addUser("tester", "456")
    p = portal.Portal(realm, [checker])

    reactor.listenTCP(port, pb.PBServerFactory(p))
    '''
    worker = Worker(master_host, master_port, node_key, worker_key)
    reactor.run()
