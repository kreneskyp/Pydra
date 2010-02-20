"""
This probably shouldn't be specific to the datasource API.
"""

import functools

def keyinit(f):
    """
    Simple decorator to be placed on a class's __init__. Instances of the
    class will have a _key attribute, which can be used to serialize and
    deseralize the class.

    Keyable classes can be nested inside other keyable classes and will still
    serialize properly.
    """

    @functools.wraps(f)
    def init(self, *args, **kwargs):
        f(self, *args, **kwargs)

        kids = []
        while args and hasattr(args[0], "key"):
            kids.append(args[0].key)
            args = args[1:]
           
        self.key = (self.__class__, kids, args, kwargs)

    return init

def keyable(c):
    """
    Simple class decorator to make a class keyable.
    """

    c.__init__ = keyinit(c.__init__)
    return c

def instance_from_key(key):
    """
    Instantiate an object from a key.
    """

    cls, kids, args, kwargs = key

    if kids:
        args = [instance_from_key(i) for i in kids] + list(args)

    return cls(*args, **kwargs)
