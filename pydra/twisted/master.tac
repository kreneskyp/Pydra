"""
This script is used by twistd (Twisted Daemon) to start the Master server for
Pydra.
"""

# replace reactor with a glib2 friendly reactor that is needed by dbus & avahi
# should be executed before any other reactor stuff to prevent from using non
# glib2 event loop which we need for dbus.
from twisted.internet import glib2reactor
glib2reactor.install()

from twisted.application import service


from pydra.config import configure_django_settings
configure_django_settings()

#setup application used by twistd
from pydra.cluster.master.master import Master
master = Master()
application = service.Application("Pydra Master")
for service in master.get_services():
    service.setServiceParent(application)
