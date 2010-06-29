import logging
import os

logger = logging.getLogger("root")

def deprecated(func=None, message=None):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""
    def wrap(func):
        def new_func(*args, **kwargs):
            if message:
                warnings.warn("Call to deprecated function %s.\n%s" % (func.__name__, message), stacklevel=2)
            else:
                warnings.warn("Call to deprecated function %s." % func.__name__, stacklevel=2)

            return func(*args, **kwargs)
        new_func.__name__ = func.__name__
        new_func.__doc__ = func.__doc__
        new_func.__dict__.update(func.__dict__)
        return new_func
    return wrap

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
