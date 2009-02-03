#! /usr/bin/python
from __future__ import with_statement

from twisted.spread import pb
from twisted.internet import reactor
from task_manager import TaskManager
from twisted.cred import credentials
import os, sys
from twisted.internet.protocol import ReconnectingClientFactory
from threading import Lock



class ControllerFactory(pb.PBClientFactory):
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
class Controller(pb.Referenceable):

    def __init__(self, master_host, master_port):
        self.id = id
        self.master = None
        self.master_host = master_host
        self.master_port = master_port
  
        print '===== STARTED Control ===='
        self.connect()

    def start(self):
        reactor.run()

    """
    Make initial connections to all Nodes
    """
    def connect(self):
        "Begin the connection process"
        factory = ReconnectingPBClientFactory()
        reactor.connectTCP(self.master_host, self.master_port, factory)
        deferred = factory.login(credentials.UsernamePassword('controller', "1234"), client=self)
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

    def status(self):
        return self.master.callRemote('status') 
