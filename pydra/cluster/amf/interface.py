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
import sys, datetime, time, hashlib

from authenticator import AMFAuthenticator
from twisted.application import internet
from twisted.cred import checkers
from twisted.internet import reactor
from twisted.python.randbytes import secureRandom
from twisted.web import server, resource


from pydra.models import pydraSettings
from pydra.cluster.auth.rsa_auth import load_crypto
from pydra.cluster.module import InterfaceModule


# init logging
import logging
logger = logging.getLogger('root')


def authenticated(fn):
    """
    decorator for marking functions as requiring authentication.

    this decorator will check the users authentication status and determine
    whether or not to call the function.  This requires that the session id
    (user) be passed with the method call.  The session_id arg isn't required
    by the function itself and will be removed from the list of args sent to 
    the real function
    """
    def new(*args):
        interface = args[0]
        user = args[-1]

        try:
            if interface.sessions[user]['auth']:
                # user is authorized - execute original function
                # strip authentication key from the args, its not needed by the
                # interface and could cause errors.
                return [fn(*(args[:-1]))]

        except KeyError:
            pass # no session yet user must go through authentication

        # user requires authorization
        return 0

    return new


class AMFFunction():
    """
    Wrapper for registered functions.  This wrapper pulls out the AMF specific
    property and then calls the mapped function
    """

    def __call__(self, _, *args):
        user = args[-1]

        try:
            if self.interface.sessions[user]['auth']:
                # user is authorized - execute original function
                # strip authentication key from the args, its not needed by the
                # interface and could cause errors.
                try:
                    ret = [self.function(*(args[:-1]))]
                except Exception, e:                    
                    import traceback
                    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
                    traceback.print_tb(exceptionTraceback, limit=10, file=sys.stdout)
                    logger.error('AMFFunction - exception in mapped function [%s] %s' % (self.function, e))
                    raise e

                return ret

        except KeyError:
            pass # no session yet user must go through authentication

        # user requires authorization
        return 0


    def __init__(self, interface, function):
        self.function = function
        self.interface = interface


class AMFInterface(InterfaceModule):
    """
    Interface for Controller.  This exposes functions to a controller.
    """

    def __init__(self, manager):
        self._services = [self.get_controller_service]

        InterfaceModule.__init__(self, manager)

        #load rsa crypto
        self.pub_key, self.priv_key = load_crypto('./master.key')

        # setup AMF gateway security.  This just uses a default user/pw
        # the real authentication happens after the AMF client connects
        self.checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
        self.checker.addUser("controller", "1234")

        # sessions - temporary sessions for all authenticated controllers
        self.sessions = {}
        self.session_cleanup = reactor.callLater(20, self.__clean_sessions)

        # Load crypto - The interface runs on the same server as the Master so
        # it can use the same key.  Theres no way with the AMF interface to
        # restrict access to localhost connections only.
        self.key_size=4096

        pub_key, priv_key = load_crypto('./master.key')
        self.priv_key_encrypt = priv_key.encrypt


    def auth(self, user, password):
        """
        Authenticate a client session.  Sessions must initially be 
        authenticated using strict security.  After that a session code can be
        used to quickly authenticate.  The session will timeout after a few 
        minutes and require the client to re-authenticate with a new session 
        code.  This model ensures that session codes are never left active for
        long periods of time.
        """
        if not self.sessions.has_key(user):
            # client has not authenticated yet.  Save session
            authenticator = AMFAuthenticator(self.checker)
            expiration = datetime.datetime.now() + datetime.timedelta(0,120)
            self.sessions[user] = {'code':password, 'expire':expiration, 'auth':False, 'challenge':None}

        return True


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


    def authenticate(self, _, user):
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

        return challenge


    def challenge_response(self, _, user, response):
        """
        Verify a response to a challenge.  A matching response allows
        this instance access to other functions that can manipulate the 
        cluster
        """
        challenge = self.sessions[user]['challenge']
        if challenge and challenge == response:
            self.sessions[user]['auth'] = True

        # destroy challenge, each challenge is one use only.
        self.sessions[user]['challenge'] = None

        return self.sessions[user]['auth']
     

    def get_controller_service(self, master):
        """
        constructs a twisted service for Controllers to connect to 
        """
        # setup controller connection via AMF gateway
        # Place the namespace mapping into a TwistedGateway:
        from pyamf.remoting.gateway.twisted import TwistedGateway
        from pyamf.remoting import gateway
        gw = TwistedGateway({ 
                        "controller": self,
                        }, authenticator=self.auth)
        # Publish the PyAMF gateway at the root URL:
        root = resource.Resource()
        root.putChild("", gw)

        #setup services
        from twisted.internet.ssl import DefaultOpenSSLContextFactory
        try:
            context = DefaultOpenSSLContextFactory('ca-key.pem', 'ca-cert.pem')
        except:
            logger.critical('Problem loading certificate required for ControllerInterface from ca-key.pem and ca-cert.pem.  Generate certificate with gen-cert.sh')
            sys.exit()

        return internet.SSLServer(pydraSettings.controller_port, server.Site(root), contextFactory=context)


    @authenticated
    def is_alive(self, _):
        """
        Remote function just for determining that Master is responsive
        """
        logger.debug('is alive')
        return 1


    def wrap_interface(self, interface):
        return AMFFunction(self, interface)
