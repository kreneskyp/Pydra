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
import os

from pydra_server.cluster.module import Module

import logging
logger = logging.getLogger('root')


class NodeInformation(Module):

    _shared = ['info','authenticated']
    
    def __init__(self, manager):
        
        self._remotes = [
            ('MASTER', 'info')
        ]

        Module.__init__(self, manager)
        self.determine_info()

    def determine_info(self):
        """
        Builds a dictionary of useful information about this Node
        """
        cores = self.detect_cores()

        self.info = {
            'cpu':2600,             # CPU MHZ per core
            'memory':3000,          # Memory allocated to the node
            'cores':cores           # Number of Cores
        }
        print 'INFO!', self.info


    def detect_cores(self):
        """
        Detect the number of core's on this Node
        """
        # Linux, Unix and MacOS:
        if hasattr(os, "sysconf"):
            if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
                # Linux & Unix:
                ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                if isinstance(ncpus, int) and ncpus > 0:
                    return ncpus
            else: # OSX:
                return int(os.popen2("sysctl -n hw.ncpu")[1].read())
        # Windows:
        if os.environ.has_key("NUMBER_OF_PROCESSORS"):
                ncpus = int(os.environ["NUMBER_OF_PROCESSORS"]);
                if ncpus > 0:
                    return ncpus
        return 1 # Default


