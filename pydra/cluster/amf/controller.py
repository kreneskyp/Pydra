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

import socket
import hashlib

from twisted.python.randbytes import secureRandom
from pyamf.remoting.client import RemotingService
from django.utils import simplejson

from pydra.cluster.auth.rsa_auth import load_crypto
import httplib


# errors for the controller
CONTROLLER_ERROR_AUTH_FAIL = -1
CONTROLLER_ERROR_DISCONNECTED = -2
CONTROLLER_ERROR_IN_REMOTE_FUNCTION = -3
CONTROLLER_ERROR_NO_RSA_KEY = -4
CONTROLLER_ERROR_UNKNOWN = -5

CONTROLLER_ERROR_MESSAGES = {
            CONTROLLER_ERROR_AUTH_FAIL:'CONTROLLER_ERROR_AUTH_FAIL',
            CONTROLLER_ERROR_DISCONNECTED:'CONTROLLER_ERROR_DISCONNECTED',
            CONTROLLER_ERROR_IN_REMOTE_FUNCTION:'CONTROLLER_ERROR_IN_REMOTE_FUNCTION',
            CONTROLLER_ERROR_NO_RSA_KEY:'CONTROLLER_ERROR_NO_RSA_KEY',
            CONTROLLER_ERROR_UNKNOWN:'CONTROLLER_ERROR_UNKNOWN',
}

class ControllerException(Exception):
    def __init__(self, code=CONTROLLER_ERROR_UNKNOWN):
        self.code = code
    
    def __repr__(self):
        print 'ControllerException (%s): %s' (self.code, CONTROLLER_ERROR_MESSAGES[self.code])


class RemoteMethodProxy():
    """
    RemoteMethodProxy is a proxy used for calling remote methods exposed over
    the AMF interface.  This proxy encapsulates authentication for the methods.
    It will automatically respond to authentication responses so that specific
    methods on the interface do not require extra code to add support.
    """

    def __init__(self, func, controller):
        self.func = func
        self.controller = controller


    def __call__(self, *args):

            # add user name to args
            new_args =  args + (self.controller.user, )

            # attempt to call func
            try:
                result = self.func(*new_args)
            except (httplib.CannotSendRequest, socket.error):
                # error connecting - retry only once then retry the function
                # if it still fails then return a failure indicating connection
                # problems.
                self.controller.connect()
                try:
                    result = self.func(*new_args)
                except (httplib.CannotSendRequest, socket.error):
                    print '[error] RemoteMethodProxy - error sending request, reconnect failed'
                    raise ControllerException(CONTROLLER_ERROR_DISCONNECTED)

            if result:
                if (result.__class__.__name__ == 'ErrorFault'):
                    print '[error] ErrorFault: ', result
                    raise ControllerException(CONTROLLER_ERROR_IN_REMOTE_FUNCTION)

                return result[0]

            # authenticate required
            if not self.controller.pub_key:
                raise ControllerException(CONTROLLER_ERROR_NO_RSA_KEY)

            if (self.controller._authenticate()):
                # authenticated reissue command
                result = self.func(*new_args)
                if (result.__class__.__name__ == 'ErrorFault'):
                    print '[error] ErrorFault: ', result
                    raise ControllerException(CONTROLLER_ERROR_IN_REMOTE_FUNCTION)

                return result[0]

            else:
                # authenticate failed
                print '[ERROR] AMFController - authentication failed'
                raise ControllerException(CONTROLLER_ERROR_AUTH_FAIL)



class AMFController(object):
    """
    AMFController - AMFController is a client for controlling the cluster via pyAMF, the actionscript
                messaging protocol.  While this app does not now and has no plans for interacting 
                with adobe flash.  The pyAMF protocol allows remoting in an asynchronous fashion.
                This is ideal for the Controller usecase.  Implementations using sockets resulted in
                connections that would not exit properly when django is run with apache. Additionally,
                Twisted reactor does not play well with django server so a twisted client is not possible

                This class is a proxy object.  All exposed methods are declared
                in the AMFInterface class.  This class will dispatch calls to
                any remote method but it is not actually aware of which methods
                are available.  The only function this class serves is to
                encapsulate connections and authentication.
    """

    services_exposed_as_properties = [
        'is_alive',
        'node_status'
    ]

    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port

        # load rsa crypto
        self.pub_key, self.priv_key = load_crypto('./master.key', False)

        print '[Info] Pydra Controller Started'
        self.connect()


    def __getattr__(self, key):
        """
        Overridden to return some functions as properties and all services 
        wrapped in a proxy object that handles authentication.
        """
        if key in self.services_exposed_as_properties:
            try:
                return RemoteMethodProxy(self.service.__getattr__(key), self)()
            except ControllerException, e:
                return e.code
            
        if key[:7] == 'remote_':
            return RemoteMethodProxy(self.service.__getattr__(key[7:]), self)

        # else its a normal method or property
        return self.__dict__[key]


    def connect(self):
        """
        Setup the client and service
        """
        self.user = hashlib.sha512(secureRandom(64)).hexdigest()
        self.password = hashlib.sha512(secureRandom(64)).hexdigest()

        self.client = RemotingService('https://127.0.0.1:18801')

        self.client.setCredentials(self.user, self.password)
        self.service = self.client.getService('controller')


    def _authenticate(self):
        # reconnect to ensure fresh credentials
        #self.connect()

        challenge = self.service.authenticate(self.user)
        challenge = challenge.__str__()

        # re-encrypt using servers key and then sha hash it.
        response_encode = self.priv_key.encrypt(challenge, None)
        response_hash = hashlib.sha512(response_encode[0]).hexdigest()
        return self.service.challenge_response(self.user, response_hash)
