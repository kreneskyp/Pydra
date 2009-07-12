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


class ModuleManager(object):
    """
    ModuleManager is used to coordinate module interactions.  It Loads them
    and handles transporting signals between emitters and listerners

    This allows modules to register for a signal that can be emitted by any 
    other component.  This allows multiple components to emit the same signal
    This system allows modules to be dependent on events rather than individual
    modules.  It also removes the need for complicated dependency checking when
    loading modules.

    IE.  Both the manual registration and autodiscovery can emit NODE_ADDED.
    """
    
    _modules = []

    # map of signals to modules that emit them. This is only used for debugging
    # purposes.  When initializing components this can be used to double check
    # registered listeners actually have someone to talk to.
    _signals = {}
    
    # map of signals to listeners.  Used for calling functions 
    # when the signal is emitted
    _listeners = {}

    # list of proeprties that are shared with the manager.  This allows them
    # to be accessed easier by other modules.  The other modules won't need
    # to know which other module supplies the property, only that it exists
    # this is a map of the property name to the class.  This is because for
    # simple types it would be a copy rather than a reference.
    _shared = {}
    
    
    # list of services that have been registered with the manager.  This is a
    # list of methods that each return a service object
    _services = []

    def emit_signal(self, signal, *args, **kwargs):
        """
        Called when a module wants to emit a signal.  It notifies
        all listeners by calling the registered

        @param signal to emit
        @param args - args list to be passed through to functions
        @param kwargs - kwargs dict to be passed through to functions
        """
        if self._listeners.has_key(signal):
            signal_listeners = self._listeners[signal]
            for function in signal_listeners:
                function(*args, **kwargs)


    def register_listener(self, signal, function):
        """
        Register a listener

        @param signal signal to listen for
        @param function function to call
        """
        try:
            self._listeners[signal].append(function)
        except KeyError:
            self._listeners[signal] = [function]


    def register_module(self, module_class):
        """
        Register a module
        """
        self._modules.append(module_class(self))


    def register_remote(self, remote, function):
        pass


    def register_service(self, service):
        """
        Register a service exposed by a module
        """
        self._services.append(service)


    def register_signal(self, signal, module):
        """
        Register a signal that a module can send
        """
        try:
            self._signals[signal].append(module)
        except KeyError:
            self._signals[signal] = [module]



    def get_shared(self, key):
        """
        Returns a shared property
        @param key to look up
        @return Value if exists, None otherwise
        """
        try:
            return self.__dict__[key]
        except KeyError:
            return None


    def set_shared(self, key, value):
        """
        Sets a shared property
        """
        self.__dict__[key] = value


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
    _listening = {}

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

        for signal in self._signals:
            manager.register_signal(signal, self)

        for signal, function in self._listening.items():
            print signal, function
            manager.register_listener(signal, function)

        for remote in self._remotes:
            manager.register_remote(*remote)

        for service in self._services: 
            manager.register_service(service)
    

    def __getattribute__(self, key):
        """
        Overridden to pass shared properties through to the manager
        """
        if key not in ('__dict__', '_shared') and key in self._shared:
            return self.manager.get_shared(key)
        return object.__getattribute__(self, key)


    def __setattribute__(self, key, value):
        """
        Overridden to pass shared property lookups through to the manager
        """
        
        if key != '__dict__' and key in self.__dict__['_shared']:
            self.manager.set_shared(key, value)
        self.__dict__[key] = value


    def emit(self, signal, *args, **kwargs):
        """
        convenience function for emitting signals    
        """
        self.manager.emit_signal(signal, *args, **kwargs)

