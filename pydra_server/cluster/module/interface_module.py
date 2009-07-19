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


from pydra_server.cluster.module.module import Module

import logging
logger = logging.getLogger('root')


class AttributeWrapper():
    """
    Wrapper for attributes that are exposed to the interface.  The wrapper
    is a callable that turns the attribute into a function that returns
    the attribute
    """    

    def __call__(self):
        return self.module.__dict__[self.attribute]

    def __init__(self, module, attribute):
       self.module = module
       self.attribute = attribute  


class InterfaceModule(Module):

    _registered_interfaces = {}

    """
    A Module that provides an Interface for Controllers.  There may be multiple
    Implementations of interfaces.  This class provides a place to stick
    common code.
    """

    def __init__(self, manager):
        self._registered_interfaces = {}   
        Module.__init__(self, manager)


    def register_interface(self, module, interface):
        """
        Registers an interface with this class.  The functions passed in are
        added to a dictionary that is searched when __getattribute__ is called.
        This allows this class to proxy calls to modules that expose functions

        only functions or properties can be exposed.  properties are exposed 
        by registering the property name.  It will be wrapped in a function

        @param interface: A function or property to expose.  Optionally it can
                          be a tuple or list of function/property and the name
                          to bind it as.
        """

        # unpack interface if it is a tuple of values
        if isinstance(interface, (tuple, list)):
            interface, name = interface
        else:
            name = None

        if isinstance(interface, (str,)):
            name = name if name else interface
            interface = AttributeWrapper(module, interface)
        else:
            name = name if name else interface.__name__

        if name in self._registered_interfaces:
            logger.debug('Binding over existing interface mapped: %s - to %s' % (name, self._registered_interfaces[name]))

        self._registered_interfaces[name] = self.wrap_interface(interface)
        logger.debug('Exposing Interface: %s - %s.%s' % (name, module, interface))


    def wrap_interface(self, interface):
        """
        Wrap the interface in an implementation specific wrapper.  This is to
        allow implementations of this class to add any logic specific to that
        API

        by default this does nothing
        """
        return interface


    def __getattribute__(self, key):
        """
        Overridden to allowed exposed functions/attributes to be looked up as if
        they were members of this instance
        """

        if key != '_registered_interfaces' and key in self._registered_interfaces:
            return self._registered_interfaces[key]

        return object.__getattribute__(self, key)
