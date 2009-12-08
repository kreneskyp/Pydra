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
import multiprocessing
import test.pystone

from pydra.cluster.module import Module

import logging
logger = logging.getLogger('root')

def get_total_memory():
    """
    Report, in kibibytes, the total system memory.
    """

    return 3000

def get_available_memory():
    """
    Report, in kibibytes, the available system memory.
    """

    return 0

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

        total_mem = get_total_memory()
        avail_mem = get_available_memory()
        cores = multiprocessing.cpu_count()
        stones = test.pystone.pystones()[1]

        self.info = {
            'total_memory': total_mem, # Total memory
            'avail_memory': avail_mem, # Available memory
            'cores':cores,             # Number of cores
            'stones':stones            # Pystone rating (higher is better)
        }

        print 'System information', self.info
