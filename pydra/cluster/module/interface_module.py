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

import datetime
import hashlib

from twisted.internet import reactor
from twisted.python.randbytes import secureRandom

from pydra.cluster.module.module import Module
from pydra.cluster.module.attribute_wrapper import AttributeWrapper

import logging
logger = logging.getLogger('root')


class InterfaceModule(Module):

    _registered_interfaces = {}

    """
    A Module that provides an Interface for Controllers.  There may be multiple
    Implementations of interfaces.  This class provides a place to stick
    common code.
    """

    def __init__(self, manager):
        self._registered_interfaces = {}   
        Module.__init__(self, manager)

        self.register_interface(self, self.authenticate, {'auth':False, \
                                                          'include_user':True})
        self.register_interface(self, self.challenge_response, {'auth':False, \
                                                        'include_user':True})

        # sessions - temporary sessions for all authenticated controllers
        self.sessions = {}
        self.session_cleanup = reactor.callLater(20, self.__clean_sessions)


    def register_interface(self, module, interface, params={}):
        """
        Registers an interface with this class.  The functions passed in are
        added to a dictionary that is searched when __getattribute__ is called.
        This allows this class to proxy calls to modules that expose functions

        only functions or properties can be exposed.  properties are exposed 
        by registering the property name.  It will be wrapped in a function

        @param interface: A function or property to expose.  Optionally it can
                          be a tuple or list of function/property and the name
                          to bind it as.
        """
        name = None

        # unpack interface if it is a tuple of values
        if isinstance(interface, (tuple, list)):
            interface, params = interface
            if 'name' in params:
                name = params['name']
                del params['name']

        if isinstance(interface, (str,)):
            name = name if name else interface
            interface = AttributeWrapper(module, interface)
        else:
            name = name if name else interface.__name__

        if name in self._registered_interfaces:
            logger.debug('Binding over existing interface mapped: %s - to %s' \
                        % (name, self._registered_interfaces[name]))

        self._registered_interfaces[name] = self.wrap_interface(interface, params)
        logger.debug('Exposing Interface: %s - %s.%s' % (name, module, interface))


    def __clean_sessions(self):
        """
        Remove session that have expired.
        """
        sessions = self.sessions
        now = datetime.datetime.now()
        for k,v in sessions.items():
            if v['expire'] <= now:
                del sessions[k]

        self.session_cleanup = reactor.callLater(20, self.__clean_sessions)


    def authenticate(self, user):
        """
        Starts the authentication process by generating a challenge string
        """
        # create a random challenge.  The plaintext string must be hashed
        # so that it is safe to be sent over the AMF service.
        challenge = hashlib.sha512(secureRandom(self.key_size/16)).hexdigest()

        # now encode and hash the challenge string so it is not stored 
        # plaintext.  It will be received in this same form so it will be 
        # easier to compare
        challenge_enc = self.priv_key_encrypt(challenge, None)
        challenge_hash = hashlib.sha512(challenge_enc[0]).hexdigest()

        self.sessions[user]['challenge'] = challenge_hash
        print 'authenticate', user, challenge_hash
        return challenge


    def challenge_response(self, user, response):
        """
        Verify a response to a challenge.  A matching response allows
        this instance access to other functions that can manipulate the 
        cluster
        """
        print 'challenge_response', user, response
        challenge = self.sessions[user]['challenge']
        if challenge and challenge == response:
            self.sessions[user]['auth'] = True

        # destroy challenge, each challenge is one use only.
        self.sessions[user]['challenge'] = None

        return self.sessions[user]['auth']


    def wrap_interface(self, interface):
        """
        Wrap the interface in an implementation specific wrapper.  This is to
        allow implementations of this class to add any logic specific to that
        API

        by default this does nothing
        """
        return interface


    def __getattribute__(self, key):
        """
        Overridden to allowed exposed functions/attributes to be looked up as if
        they were members of this instance
        """

        if key != '_registered_interfaces' and key in self._registered_interfaces:
            return self._registered_interfaces[key]

        return object.__getattribute__(self, key)
