from pyamf.remoting.client import RemotingService
from django.utils import simplejson
import socket

"""
AMFController - AMFController is a client for controlling the cluster via pyAMF, the actionscript
             messaging protocol.  While this app does not now and has no plans for interacting 
             with adobe flash.  The pyAMF protocol allows remoting in an asynchronous fashion.  
             This is ideal for the Controller usecase.  Implementations using sockets resulted in
             connections that would not exit properly when django is run with apache. Additionally, 
             Twisted reactor does not play well with django server so a twisted client is not possible
"""
class AMFController(object):

    services_exposed_as_properties = [
        'is_alive',
        'node_status'
    ]

    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port

        print '[Info] Pydra Controller Started'
        self.connect()

    """
    Overridden to lookup some functions as properties
    """
    def __getattr__(self, key):
        #check to see if this is a function acting as a property
        if key in self.services_exposed_as_properties:
            return self.__class__.__dict__['remote_%s' % key](self)

        return self.__dict__[key]


    """
    Setup the client and service
    """
    def connect(self):
        #connect
        self.client = RemotingService('http://127.0.0.1:18801')
        self.client.setCredentials('controller','1234')
        self.service = self.client.getService('controller')


    """
    Simple ping just to see if connection is active
    """
    def remote_is_alive(self):
        try:
            return self.service.is_alive()
        except socket.error:
            # need to reconnect after a socket error
            self.connect()
            return 0


    """
    Returns a json'ified list of status for nodes/workers
    """
    def remote_node_status(self):
        try:
            ret = self.service.node_status()
            print ret
            return simplejson.dumps(ret)
        except socket.error:
            # need to reconnect after a socket error
            self.connect()
            return 0
