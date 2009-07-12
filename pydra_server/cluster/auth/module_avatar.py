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

from rsa_auth import RSAAvatar

import logging
logger = logging.getLogger('root')


class ModuleAvatar(RSAAvatar):
    """
    Avatar that aggregates methods exposed by Pydra Modules.  The methods will
    be stored in a dictionary mapping the method names back to the module that
    exposed them
    """

    _manager = None

    def __init__(self, manager, *args, **kwargs):
        self._manager = manager
        RSAAvatar.__init__(self, *args, **kwargs)


    def __getattribute__(self, key):
        """
        Overridden to lookup remoted methods from modules
        """
        if 'perspective_' == key[:12]:
            try:
                module = self._manager.__dict__[key[:12]]
                return module.__dict__[key[:12]]
            except KeyError:
                # key not found
                pass

        # default to attributes of this class
        return object.getattribute(key)
