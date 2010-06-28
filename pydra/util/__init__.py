# import so it can be a part of the module
from deprecated import deprecated

import logging
import os

logger = logging.getLogger("root")

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
