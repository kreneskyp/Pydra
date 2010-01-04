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

"""
Pydra configuration - This file is used to specify paths used by pydra.  These
are used to deal with platform specific oddities that occur when installing a
package.  This file should be importable as pydra.config allowing these paths
to be located.
"""
import os
import sys

CONFIG_DIR = '/etc/pydra'


def configure_django_settings(settings='pydra_settings'):
    """
    Configures sys.path and DJANGO_SETTINGS_MODULE for Pydra.  Because Pydra
    components are run as applications rather than web-apps these things must
    be setup for django to find them.
    """
    sys.path.append(CONFIG_DIR)

    if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
        os.environ['DJANGO_SETTINGS_MODULE'] = settings


def load_settings():
    """
    Helper function for adding pydra settings to the python path
    """
    sys.path.append(CONFIG_DIR)
    import pydra_settings
    return pydra_settings

