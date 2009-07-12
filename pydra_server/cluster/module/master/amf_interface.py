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

from twisted.application import internet
from twisted.cred import checkers
from twisted.web import server, resource


from pydra_server.models import pydraSettings
from pydra_server.cluster.amf.interface import AMFInterface
from pydra_server.cluster.auth.rsa_auth import load_crypto
from pydra_server.cluster.module import Module


# init logging
import settings
from pydra_server.logging.logger import init_logging
logger = init_logging(settings.LOG_FILENAME_MASTER)


class AMFInterfaceModule(Module):
   
    def __init__(self, manager):
        self._services = [self.get_controller_service]

        #load rsa crypto
        self.pub_key, self.priv_key = load_crypto('./master.key')

        Module.__init__(self, manager)
       

    def get_controller_service(self, master):
        """
        constructs a twisted service for Controllers to connect to 
        """
        
        # setup AMF gateway security.  This just uses a default user/pw
        # the real authentication happens after the AMF client connects
        checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
        checker.addUser("controller", "1234")

        # setup controller connection via AMF gateway
        # Place the namespace mapping into a TwistedGateway:
        from pyamf.remoting.gateway.twisted import TwistedGateway
        from pyamf.remoting import gateway
        interface = AMFInterface(self, checker)
        gw = TwistedGateway({ 
                        "controller": interface,
                        }, authenticator=interface.auth)
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
