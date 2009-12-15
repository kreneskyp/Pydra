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

    total = 0

    for line in open("/proc/meminfo"):
        if line.startswith("MemTotal"):
            chaff, total, chaff = line.split()
            break
    return int(total)

def get_available_memory():
    """
    Report, in kibibytes, the available system memory.
    """

    free, buffered, cached = 0, 0, 0

    for line in open("/proc/meminfo"):
        if line.startswith("MemFree"):
            chaff, free, chaff = line.split()
        elif line.startswith("Buffers"):
            chaff, buffered, chaff = line.split()
        elif line.startswith("Cached"):
            chaff, cached, chaff = line.split()

    return int(free) + int(buffered) + int(cached)

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

        total_mem = get_total_memory() / 1024
        avail_mem = get_available_memory() / 1024
        cores = multiprocessing.cpu_count()
        stones = test.pystone.pystones()[1]

        self.info = {
            'total_memory': total_mem, # Total memory
            'avail_memory': avail_mem, # Available memory
            'cores':cores,             # Number of cores
            'stones':stones            # Pystone rating (higher is better)
        }

        print 'System information', self.info
