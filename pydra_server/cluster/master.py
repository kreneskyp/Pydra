#!/usr/bin/env python

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

from __future__ import with_statement

# ==========================================================
# Setup django environment 
# ==========================================================

import sys
import os

#python magic to add the current directory to the pythonpath
sys.path.append(os.getcwd())

#
if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
    os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

# ==========================================================
# Done setting up django environment
# ==========================================================
import os, sys
import time


# should be executed before any other reactor stuff to prevent from using non
# glib2 event loop which we need for dbus
from twisted.internet import glib2reactor
glib2reactor.install()

from threading import Lock


from twisted.application import service
from twisted.internet.error import AlreadyCalled

from twisted.cred import credentials
import settings

from pydra_server.cluster.constants import *

from pydra_server.cluster.module.module import ModuleManager
from pydra_server.cluster.module.master import NodeConnectionManager, WorkerConnectionManager, TaskScheduler, AutoDiscoveryModule, AMFInterfaceModule
from pydra_server.models import pydraSettings

# init logging
from pydra_server.logging.logger import init_logging
logger = init_logging(settings.LOG_FILENAME_MASTER)


class Master(object):
    """
    Master is the server that controls the cluster.  There must be one and only one master
    per cluster (for now).  It will direct and delegate work taking place on the Nodes and Workers
    """

    def __init__(self):
        logger.info('====== starting master ======')

        #locks
        self._lock = Lock()         #general lock, use when multiple shared resources are touched
        self._lock_queue = Lock()   #for access to _queue

        #load rsa crypto
        from pydra_server.cluster.auth.rsa_auth import load_crypto
        self.pub_key, self.priv_key = load_crypto('./master.key')
        

        # initialize modules
        self.module_manager = ModuleManager()

        modules = [
            AutoDiscoveryModule,
            NodeConnectionManager,
            WorkerConnectionManager,
            TaskScheduler,
            AMFInterfaceModule
        ]
        map(self.module_manager.register_module, modules)

        self.module_manager.emit_signal('MASTER_INIT')


    def get_services(self):
        """
        Get the service objects used by twistd.  The services are exposed by
        modules.  This method just calls all the mapped functions used for
        creating the services
        """
        return [service(self) for service in self.module_manager._services]


#setup application used by twistd
master = Master()

application = service.Application("Pydra Master")

for service in master.get_services():
    print 'Starting service'
    service.setServiceParent(application)

