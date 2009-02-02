#! /usr/bin/python
from __future__ import with_statement

#
# Setup django environment 
#
if __name__ == '__main__':
    import sys
    import os

    #python magic to add the current directory to the pythonpath
    sys.path.append(os.getcwd())

    #
    if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
        os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'


from threading import Lock
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
class NodeServer:
    def __init__(self):
        self.workers = {}
        self.port_base = 11881
        self.host='localhost'
        self.node_key = None
        self.initialized = False
        self.__lock = Lock()
        #load tasks that are cached locally
        self.available_tasks = {}

        # get information about the server
        self.determine_info()

        print '[Info] - Starting server on port %s' % self.port_base


    """
    Builds a dictionary of useful information about this Node
    """
    def determine_info(self):
        cores = 1 #self.detect_cores()

        self.info = {
            'cpu':2600,             # CPU MHZ per core
            'memory':3000,          # Memory allocated to the node
            'cores':cores           # Number of Cores
        }


    """
    Initializes the node so it ready for use.  Workers will not be started
    until the master makes this call.  After a node is initialized workers
    should be able to reconnect if a connection is lost
    """
    def init_node(self, master_host, master_port, node_key):

        # only initialize the node if it has not been initialized yet.
        # its possible for the server to be restarted without affecting
        # the state of the nodes
        if not self.initialized:
            with self.__lock:
                self.master_host = master_host
                self.master_port = master_port
                self.node_key = node_key

                #start the workers
                self.start_workers()

                self.initialized = True

    """
    Starts all of the workers.  By default there will be one worker for each core
    """
    def start_workers(self):
        self.pids = [
            Popen(["python", "cluster/worker.py", self.master_host, str(self.master_port), self.node_key, '%s:%s' % (self.node_key, i)]).pid 
            for i in range(self.info['cores'])
            ]

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
        avatar = MasterAvatar(avatarID)
        avatar.server = self.server
        avatar.attached(mind)
        return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)


class MasterAvatar(pb.Avatar):
    def __init__(self, name):
        self.name = name
        print '   [Node]: master connected (node)'

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

    """
    Initializes a node.  The server sends its connection information and
    credentials for the node
    """
    def perspective_init(self, master_host, master_port, node_key):
        self.server.init_node(master_host, master_port, node_key)


realm = ClusterRealm()
realm.server = NodeServer()
checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
checker.addUser("tester", "1234")
p = portal.Portal(realm, [checker])

reactor.listenTCP(11880, pb.PBServerFactory(p))
reactor.run()
