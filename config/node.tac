"""
This script is used by twistd (Twisted Daemon) to start the NodeServer for
Pydra.
"""

from twisted.application import service

from pydra.config import configure_django_settings
configure_django_settings()
from pydra.cluster.node.node import NodeServer

#root application object
application = service.Application('Pydra Node')

#create node server
node_server = NodeServer()

# attach services
for service in node_server.get_services():
    service.setServiceParent(application)
