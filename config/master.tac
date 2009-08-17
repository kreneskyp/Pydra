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


import time


# should be executed before any other reactor stuff to prevent from using non
# glib2 event loop which we need for dbus
from twisted.internet import glib2reactor
glib2reactor.install()

from twisted.application import service
from pydra.cluster.master.master import Master

#setup application used by twistd
master = Master()
application = service.Application("Pydra Master")
for service in master.get_services():
    service.setServiceParent(application)
