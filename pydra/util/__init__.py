"""
Common utility functions used by multiple parts of Pydra.
"""

__docformat__ = "restructuredtext"

import functools
import logging
import os
import warnings

logger = logging.getLogger("root")

def deprecated(message="Generic warning of impending breakage"):
    """
    This decorator will emit a warning when its wrapped function is called.

    It should be customized by passing a message to the primary
    decorator, as follows:

    >>> @deprecated("Stupid function, consider using bar() instead")
    ... def foo():
    ...     print "I'm kind of stupid, sorry."
    """

    def secondary_decorator(f):
        @functools.wraps(f)
        def warning(*args, **kwargs):
            warnings.warn(message, DeprecationWarning, 2)
            return f(*args, **kwargs)

        return warning

    return secondary_decorator

def makedirs(path):
    """
    Pydra occasionally needs directories. This function creates them in a
    safe and generally correct way.
    """

    try:
        os.makedirs(path)
    except OSError, e:
        if e.errno == 17:
            # OK; directory already exists
            pass
        else:
            logger.critical("Couldn't create directory %s!" % path)
            raise
