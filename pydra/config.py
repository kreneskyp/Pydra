"""
Pydra configuration - This file is used to specify paths used by pydra.  These
are used to deal with platform specific oddities that occur when installing a
package.  This file should be importable as pydra.config allowing these paths
to be located.
"""
import sys

CONFIG_DIR = '/etc/pydra'
LOGGING_DIR = '/var/log/pydra'
RUNTIME_FILES_DIR = '/var/lib/pydra'

def load_settings():
    """
    Helper function for adding settings to the python path
    """
    sys.path.append(CONFIG_DIR)
    import pydra_settings
    return pydra_settings
