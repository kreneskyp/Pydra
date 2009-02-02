#! /usr/bin/python

from zope.interface import implements
from twisted.cred import portal, checkers
from twisted.spread import pb
from twisted.internet import reactor
import os
from subprocess import Popen

"""
Node - A Node manages a server in your cluster.  There is one instance of Node running per server.
      Node will spawn worker processes for each core available on your machine.  This allows some
      central control over what happens on the node.
"""
class Node:
    def __init__(self):
        self.workers = {}
        self.port_base = 11000
        self.host='localhost'

        #load tasks that are cached locally
        self.available_tasks = {}

        # get information about the server
        self.determine_info()

        # start the appropriate number of workers
        self.start_workers()

        print '===== STARTED ===='


    """
    Builds a dictionary of useful information about this Node
    """
    def determine_info(self):
        cores = 1 #self.detect_cores()
        worker_list = [self.port_base+i for i in range(cores)]

        self.info = {
            'workers':worker_list,  # List of node ports
            'cpu':2600,             # CPU MHZ per core
            'memory':3000,          # Memory allocated to the node
            'cores':cores           # Number of Cores
        }


    """
    Starts all of the workers.  By default there will be one worker for each core
    """
    def start_workers(self):
        self.pids = [Popen(["python", "cluster/worker.py", self.host, str(port)]).pid for port in self.info['workers']]
        print "PIDs:", self.pids


    """
    Detect the number of core's on this Node
    """
    def detect_cores(self):
        """
        Detects the number of cores on a system. Cribbed from pp.
        """
        # Linux, Unix and MacOS:
        if hasattr(os, "sysconf"):
            if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
                # Linux & Unix:
                ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                if isinstance(ncpus, int) and ncpus > 0:
                    return ncpus
            else: # OSX:
                return int(os.popen2("sysctl -n hw.ncpu")[1].read())
        # Windows:
        if os.environ.has_key("NUMBER_OF_PROCESSORS"):
                ncpus = int(os.environ["NUMBER_OF_PROCESSORS"]);
                if ncpus > 0:
                    return ncpus
        return 1 # Default


class ClusterRealm:
    implements(portal.IRealm)
    def requestAvatar(self, avatarID, mind, *interfaces):
        assert pb.IPerspective in interfaces
        avatar = Master(avatarID)
        avatar.server = self.server
        avatar.attached(mind)
        return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)

class Master(pb.Avatar):
    def __init__(self, name):
        self.name = name
        print '   client connected (node)'

    def attached(self, mind):
        self.remote = mind
    def detached(self, mind):
        self.remote = None

    # returns the status of this node
    def perspective_status(self):
        pass

    # returns the list of available tasks
    def perspective_task_list(self):
        pass

    # Returns a dictionary of useful information about this node
    def perspective_info(self):
        return self.server.info

realm = ClusterRealm()
realm.server = Node()
checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
checker.addUser("tester", "1234")
p = portal.Portal(realm, [checker])

reactor.listenTCP(18801, pb.PBServerFactory(p))
reactor.run()
