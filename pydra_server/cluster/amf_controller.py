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

from pyamf.remoting.client import RemotingService
from django.utils import simplejson
import socket


class AMFController(object):
    """
    AMFController - AMFController is a client for controlling the cluster via pyAMF, the actionscript
                messaging protocol.  While this app does not now and has no plans for interacting 
                with adobe flash.  The pyAMF protocol allows remoting in an asynchronous fashion.  
                This is ideal for the Controller usecase.  Implementations using sockets resulted in
                connections that would not exit properly when django is run with apache. Additionally, 
                Twisted reactor does not play well with django server so a twisted client is not possible
    """

    services_exposed_as_properties = [
        'is_alive',
        'node_status'
    ]

    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port

        print '[Info] Pydra Controller Started'
        self.connect()


    def __getattr__(self, key):
        """
        Overridden to lookup some functions as properties
        """
        #check to see if this is a function acting as a property
        if key in self.services_exposed_as_properties:
            return self.__class__.__dict__['remote_%s' % key](self)

        return self.__dict__[key]


    def connect(self):
        """
        Setup the client and service
        """
        #connect
        self.client = RemotingService('http://127.0.0.1:18801')
        self.client.setCredentials('controller','1234')
        self.service = self.client.getService('controller')


    def remote_is_alive(self):
        """
        Simple ping just to see if connection is active
        """
        try:
            return self.service.is_alive()
        except socket.error:
            # need to reconnect after a socket error
            self.connect()
            return 0


    def remote_node_status(self):
        """
        Returns a json'ified list of status for nodes/workers
        """
        try:
            ret = self.service.node_status()
            print ret
            return simplejson.dumps(ret)
        except socket.error:
            # need to reconnect after a socket error
            self.connect()
            return 0


    def remote_list_tasks(self):
        """
        Returns a list of tasks that can be run
        """
        try:
            ret = self.service.list_tasks()
            return ret
        except socket.error:
            # need to reconnect after a socket error
            self.connect()
            return 0


    def remote_list_queue(self):
        """
        Returns a list of tasks that can be run
        """
        try:
            ret = self.service.list_queue()
            print ret
            return ret
        except socket.error:
            # need to reconnect after a socket error
            self.connect()
            return 0


    def remote_list_running(self):
        """
        Returns a list of tasks that can be run
        """
        try:
            ret = self.service.list_running()
            print ret
            return ret
        except socket.error:
            # need to reconnect after a socket error
            self.connect()
            return 0


    def remote_run_task(self, key):
        """
        Requests a task be run
        """
        try:
            ret = self.service.run_task(key)
            return ret
        except socket.error:
            # need to reconnect after a socket error
            self.connect()
            return 0


    def remote_cancel_task(self, id):
        """
        Cancels a task.  This is used for tasks in the queue and 
        tasks that are running.  Its the same function because that
        state can change before this method reaches the remote server
        """
        try:
            ret = self.service.cancel_task(id)
            print ret
            return ret
        except socket.error:
            # need to reconnect after a socket error
            self.connect()
            return 0
