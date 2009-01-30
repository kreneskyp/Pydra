#! /usr/bin/python

from zope.interface import implements
from twisted.cred import portal, checkers
from twisted.spread import pb
from twisted.internet import reactor
from task_manager import TaskManager
import os, sys

"""
Worker - The Worker is the workhorse of the cluster.  It sits and waits for Tasks and SubTasks to be executed
         Each Task will be run on a single Worker.  If the Task or any of its subtasks are a ParallelTask 
         the first worker will make requests for work to be distributed to other Nodes
"""
class Worker:

    def __init__(self, master_host, master_port, node_key, worker_key):
        self.id = id
        self.__task = None
        self.master = None

        #load tasks that are cached locally
        self.task_manager = TaskManager()
        self.task_manager.autodiscover()
        self.available_tasks = self.task_manager.registry

        print '===== STARTED Worker ===='
        print self.available_tasks


    """
     Runs a task on this worker
    """
    def run_task(self, key, args=None):
        #Check to ensure this worker is not already busy.
        # The Master should catch this but lets be defensive.
        if self.__task:
            return "FAILURE THIS WORKER IS ALREADY RUNNING A TASK"
        self.__task = key
        
        print 'Starting task: %s' % key
        return self.available_tasks[key].start(args, callback=self.work_complete)

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
        self.master.send_results(_results)

    """
    Requests a work unit be handled by another worker in the cluster
    """
    def request_worker(self, subtask_key, args):
        self.master.request_worker(self.__task, subtask_key, args)


class ClusterRealm:
    implements(portal.IRealm)
    def requestAvatar(self, avatarID, mind, *interfaces):
        assert pb.IPerspective in interfaces
        avatar = MasterWorker(avatarID)
        avatar.server = self.server
        avatar.attached(mind)
        self.server.master = avatar
        return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)

"""
Avatar representing the Master's control over a worker.  There should
only ever be one instance of this class per worker.
"""
class MasterWorker(pb.Avatar):
    def __init__(self, name):
        self.name = name
        print '   client connected (worker)'

    def attached(self, mind):
        print mind
        self.remote = mind

    def detached(self, mind):
        self.remote = None
        self.server.master = None

    # returns the status of this node
    def perspective_status(self):
        return self.server.status()

    # returns the list of available tasks
    def perspective_task_list(self):
        return self.server.available_tasks.keys()

    # run a task
    def perspective_run_task(self, key, args=None):
        return self.server.run_task(key, args)

    def send_results(self, results):
        print 'results sent! [%s]' % results
        self.remote.callRemote("send_results", results)

    def request_worker(self, task_key, subtask_key, args):
        deferred = self.remote.callRemote("request_worker", subtask_key, args)
        #deferred.addCallback()


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