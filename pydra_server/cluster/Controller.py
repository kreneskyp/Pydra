#! /usr/bin/python

"""
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

from twisted.spread import pb
from twisted.internet import reactor
from task_manager import TaskManager
from twisted.cred import credentials
import os, sys
from twisted.internet.protocol import ReconnectingClientFactory
from threading import Lock



class ControllerFactory(pb.PBClientFactory):
    def clientConnectionLost(self, connector, reason):
        pb.PBClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        pb.PBClientFactory.clientConnectionFailed(self, connector, reason)




"""
Worker - The Worker is the workhorse of the cluster.  It sits and waits for Tasks and SubTasks to be executed
         Each Task will be run on a single Worker.  If the Task or any of its subtasks are a ParallelTask 
         the first worker will make requests for work to be distributed to other Nodes
"""
class Controller(pb.Referenceable):

    def __init__(self, master_host, master_port):
        self.master = None
        self.master_host = master_host
        self.master_port = master_port
        self.connected = False

        print '[Info] Initializing Pydra Controller'
        self.connect()

    def start(self):
        print '[Info] Starting Pydra Controller'
        reactor.run()

    """
    Make initial connections to all Nodes
    """
    def connect(self):
        "Begin the connection process"
        factory = ControllerFactory()
        reactor.connectTCP(self.master_host, self.master_port, factory)
        deferred = factory.login(credentials.UsernamePassword('controller', "1234"), client=self)
        deferred.addCallback(self.connection_success)
        deferred.addErrback(self.connection_failed)

    """
    Callback called when connection to master is made
    """
    def connection_success(self, result):
        print '[info] Connected to master'
        self.master = result
        self.connected = True

    """
    Callback called when conenction to master fails
    """
    def connection_failed(self, result, *arg, **kw):
        print '[error] Connected to master failed'
        print result

    def status(self):
        return self.master.callRemote('status') 
