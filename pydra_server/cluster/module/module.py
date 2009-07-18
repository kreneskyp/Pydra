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

import logging
logger = logging.getLogger('root')


class Module(object):
    """
    Base class for definining a pydra module.  A module is plugin that adds
    functionality to Pydra.  

    A Module may register:
        * Signals   - Signals that it emits, defined as a list of signals

        * Listeners - Signals that it will listen to.defined as a dictionary of
                      signal names and functions

        * Remotes   - Functions that a remote service can use.  Defined as a
                      list of functions and the avatar class they belong to.  
                      There may be more than one avatar.
    """

    # list of signals this module will emit
    _signals = []

    # mapping of signals and the functions this module will invoke upon 
    # receiving that signal
    _listeners = {}

    # list of functions this module adds to avatar classes 
    _remotes = []

    # list of functions this module adds to the controller interface
    _interfaces = []

    # list of services this module publishes.
    _services = []

    # list of properties this module shares
    _shared = []

    def __init__(self, manager):
        """
        Initializes the module.  All of the functionality provided by this
        module is registered with the ModuleManager
        """
        self.manager = manager
    

    def __getattribute__(self, key):
        """
        Overridden to pass shared properties through to the manager
        """
        if key not in ('__dict__', '_shared') and key in self._shared:
            val =  self.manager.get_shared(key)
            #print 'GETTING: ', key, val
            return val
        return object.__getattribute__(self, key)


    def __setattr__(self, key, value):
        """
        Overridden to pass shared property lookups through to the manager
        """
        if key not in ('__dict__', '_shared') and key in self._shared:
            #print ' SETTING: ', key, value
            self.manager.set_shared(key, value)

        self.__dict__[key] = value


    def emit(self, signal, *args, **kwargs):
        """
        convenience function for emitting signals    
        """
        print 'EMIT %s: %s %s' % (signal, args, kwargs)
        self.manager.emit_signal(signal, *args, **kwargs)

