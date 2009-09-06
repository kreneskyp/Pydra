"""
Pydra configuration - This file is used to specify paths used by pydra.  These
are used to deal with platform specific oddities that occur when installing a
package.  This file should be importable as pydra.config allowing these paths
to be located.
"""
import os
import sys

CONFIG_DIR = '/etc/pydra'

def load_settings():
    """
    Helper function for adding pydra settings to the python path
    """
    sys.path.append(CONFIG_DIR)
    import pydra_settings
    return pydra_settings


def configure_django_settings():
    """
    Configures sys.path and DJANGO_SETTINGS_MODULE for Pydra.  Because Pydra
    components are run as applications rather than web-apps these things must
    be setup for django to find them.
    """
    sys.path.append(CONFIG_DIR)

    if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
        os.environ['DJANGO_SETTINGS_MODULE'] = 'pydra_settings'

