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
import settings
from dbus.mainloop.glib import DBusGMainLoop
import dbus, avahi

from pydra_server.cluster.module import Module
from pydra_server.models import pydraSettings

# init logging
import logging
logger = logging.getLogger('root')

class AutoDiscoveryModule(Module):

    _signals = [
        'NODE_ADDED',
    ]

    _shared = [
        'known_nodes'
    ]


    def __init__(self, manager):

        self._interfaces = [self.list_known_nodes]

        Module.__init__(self, manager)

        self.known_nodes = set()
        self.autodiscovery()



    def autodiscovery(self, callback=None):
        """
        set up the dbus loop, and add the callbacks for adding nodes on the fly

        based on http://avahi.org/wiki/PythonBrowseExample
        """
        def service_resolved(*args):
            # at this point we have all the info about the node we need
            if pydraSettings.multicast_all:

                # add the node (without the restart)
                Node.objects.create(host=args[7], port=args[8])
                self.connect()
            else:
                self.known_nodes.add((args[7], args[8]))

        def print_error(*args):
            logger.info("Couldn't resolve avahi name: %s" % str(args))

        def node_found(interface, protocol, name, stype, domain, flags):
            if flags & avahi.LOOKUP_RESULT_LOCAL:
                    # local service, skip
                    pass

            server.ResolveService(interface, protocol, name, stype,
                domain, avahi.PROTO_UNSPEC, dbus.UInt32(0),
                reply_handler=service_resolved, error_handler=print_error)


        # initialize dbus stuff needed for discovery
        loop = DBusGMainLoop()

        bus = dbus.SystemBus(mainloop=loop)

        server = dbus.Interface( bus.get_object(avahi.DBUS_NAME, '/'),
                'org.freedesktop.Avahi.Server')

        sbrowser = dbus.Interface(bus.get_object(avahi.DBUS_NAME,
                server.ServiceBrowserNew(avahi.IF_UNSPEC,
                    avahi.PROTO_UNSPEC, '_pydra._tcp', 'local', dbus.UInt32(0))),
                avahi.DBUS_INTERFACE_SERVICE_BROWSER)

        sbrowser.connect_to_signal("ItemNew", node_found)


    def list_known_nodes(self, _):
        """
        list know_nodes
        """
        # cast to list, doesn't seem to digest set
        return list(self.master.known_nodes)


