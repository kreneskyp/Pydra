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

class ModuleManager():
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

    
    def register_module(self, module_class):
        self._modules.append(module_class(self))
    

    def register_signal(self, signal, module):
        """
        Register a signal that a module can send
        """
        try:
            self._signals[signal].append(module)
        except KeyError:
            self._signals[signal] = [module]


    def listen(self, signal, function):
        """
        Register a listener

        @param signal signal to listen for
        @param function function to call
        """
        try:
            self._listeners[signal].append(function)
        except KeyError:
            self._listeners[signal] = [function]


    def emit_signal(self, signal, *args, **kwargs)
        """
        Called when a module wants to emit a signal.  It notifies
        all listeners by calling the registered

        @param signal to emit
        @param args - args list to be passed through to functions
        @param kwargs - kwargs dict to be passed through to functions
        """
        signal_listeners = self._listeners[signal]
        for function in signal_listeners:
            function(*args, **kwargs)



class Module():
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

    # list of functions that this module adds to avatar classes 
    _remotes = []
        
    def __init__(self, manager):
        """
        Initializes the 
        """
        self.manager = manager

        for signal in self._signals:
            manager.register_signal(signal, self)

        for signal, function in _listening.items():
            manager.listen(signal, function)

        for remote in self._remotes:
            manager.register_remote(remote)
    

    def emit(self, signal, *args, **kwargs):
        """
        convenience function for emitting signals    
        """
        self.manager.emit_signal(signal, *args, **kwargs)

