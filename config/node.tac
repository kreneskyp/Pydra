# ==========================================================
# Setup django environment 
# ==========================================================
import os
import sys
from pydra.config import CONFIG_DIR

# python magic to add the config directory to the python path.
# the config directory contains the django settings file
sys.path.append(CONFIG_DIR)

#
if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
    os.environ['DJANGO_SETTINGS_MODULE'] = 'pydra_settings'
# ==========================================================
# Done setting up django environment
# ==========================================================

from twisted.application import service
from pydra.cluster.node.node import NodeServer

#root application object
application = service.Application('Pydra Node')

#create node server
node_server = NodeServer()

# attach service
for service in node_server.get_services():
    service.setServiceParent(application)
