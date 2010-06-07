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
import StringIO
import sys
import traceback

from pydra.cluster.controller.amf.authenticator import AMFAuthenticator
import simplejson
from twisted.application import internet
from twisted.cred import checkers
from twisted.internet.defer import Deferred
from twisted.web import server, resource, http, http_headers
from twisted.web.error import ForbiddenResource, NoResource, Error

from pydra.cluster.auth.rsa_auth import load_crypto
from pydra.cluster.module import InterfaceModule
import pydra_settings

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


def deferred_response(response, request):
    """
    Generic callback for web requests receive a Deferred from the mapped
    function.  This simply json encodes the response and passes it to the
    request
    
    @param response - response from deferred function
    @param request - http request object from original call
    """
    request.write(simplejson.dumps(response))
    request.finish()


class FunctionResource(resource.Resource):
    """
    An extension of a twisted resource that exposes a mapped function.
    
    All function expect arguments to be encoded as a JSON string and stored
    in the post as 'args' and 'kwargs'.  This allows values to be passed in
    retaining their types and structure (to the degree that json allows)
    
    results will similarly be returned as a JSON encoded string.  Errors will
    result in a 500 error with details within the JSON encoded string.
    """
    def __init__(self, interface, function, params={}):
        self.interface = interface
        self.function = function
        self.requires_auth = params['auth'] if 'auth' in params else True
        self.include_user = params['include_user'] if 'include_user' in \
                                                            params else False

    def render(self, req):
        user = req.getSession().uid
        args = req.args
        args = simplejson.loads(args['args'][0]) if 'args' in args else []
        kwargs = simplejson.loads(args['kwargs'][0]) if 'kwargs' in args else {}
        password = args['pass'] if 'pass' in args else None

        if not self.interface.sessions.has_key(user):
            # client has not authenticated yet.  Save session
            expiration = datetime.datetime.now() + datetime.timedelta(0,120)
            self.interface.sessions[user] = {'code':password, \
                                             'expire':expiration, \
                                             'auth':False, \
                                            'challenge':None}

        if self.interface.sessions[user]['auth'] or not self.requires_auth:
            # user is authorized - execute original function
            # strip authentication key from the args, its not needed by the
            # interface and could cause errors.
            try:
                if self.include_user:
                    results = self.function(user, *args, **kwargs)
                else:
                    results = self.function(*args, **kwargs)

                # if the method returns a deferred hook up
                if isinstance(results, (Deferred)):
                    results.addCallback(deferred_response, req)
                    return server.NOT_DONE_YET
                else:
                    return simplejson.dumps(results)

            except Exception, e:
                chaff, chaff, tb = sys.exc_info()

                buf = StringIO.StringIO()
                traceback.print_tb(tb, limit=10, file=buf)
                traces = buf.getvalue()
                buf.close()

                sys.stdout.write(traces)

                logger.error('FunctionResource - exception in mapped function \
                             [%s] %s' % (self.function, e))
                error = simplejson.dumps({'exception':str(e), \
                                          'traceback':traces})
                req.setResponseCode(500)
                req.setHeader("content-type", "text/html")
                return error

        # user requires authorization
        req.setResponseCode(401)
        req.setHeader("content-type", "text/html")
        return 'authentication required for this method'


class InterfaceResource(resource.Resource):
    """
    Resource that maps all registered interfaces to children of this resource
    """
    def __init__(self, module):
        self.module = module
    
    def getChildWithDefault(self, path, request):
        if path == '':
            return self
        try:
            return self.module._registered_interfaces[path]
        except KeyError:
            return NoResource('method does not exist')

    def render(self, request):
        return simplejson.dumps(self.module._registered_interfaces.keys())


class TwistedWebInterface(InterfaceModule):
    """
    Interface for Controller.  This exposes functions to a controller via
    Twisted.web2.  Functions are mapped to urls using twisted web.
    """

    def _register(self, manager):
        InterfaceModule._register(self, manager)
        self._services = [self.get_controller_service]
        
        #load rsa crypto
        self.pub_key, self.priv_key = load_crypto('%s/master.key' % \
                                        pydra_settings.RUNTIME_FILES_DIR)
        self.priv_key_encrypt = self.priv_key.encrypt

        # setup security.  This just uses a default user/pw
        # the real authentication happens after the AMF client connects
        self.checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
        self.checker.addUser("controller", "1234")

        # Load crypto - The interface runs on the same server as the Master so
        # it can use the same key.  Theres no way with the AMF interface to
        # restrict access to localhost connections only.
        self.key_size=4096

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
            self.sessions[user] = {'code':password, 'expire':expiration, \
                                   'auth':False, 'challenge':None}

        return True


    def get_controller_service(self, master):
        """
        constructs a twisted service for Controllers to connect to 
        """
        root = InterfaceResource(self)

        #setup services
        from twisted.internet.ssl import DefaultOpenSSLContextFactory
        try:
            key = '%s/ca-key.pem' % pydra_settings.RUNTIME_FILES_DIR
            cert = '%s/ca-cert.pem' % pydra_settings.RUNTIME_FILES_DIR
            context = DefaultOpenSSLContextFactory(key, cert)
        except:
            logger.critical('Problem loading certificate required for \
                            ControllerInterface from ca-key.pem and \
                            ca-cert.pem.  Generate certificate with \
                            gen-cert.sh')
            sys.exit()

        return internet.SSLServer(pydra_settings.CONTROLLER_PORT, \
                                  server.Site(root), contextFactory=context)
    
    
    def wrap_interface(self, interface, params={}):
        """
        Wraps all registered interfaces in an InterfaceResource.  This allows
        all interfaces to conform to twisted.web2 API.
        """
        return FunctionResource(self, interface, params)
