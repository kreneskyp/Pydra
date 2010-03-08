"""
This probably shouldn't be specific to the datasource API.
"""

import functools

def save_class(cls):
    """
    Serialize a class handle.
    """

    return cls.__name__, cls.__module__

def restore_class(cls, g={}, l={}):
    """
    Deserialize a class identifier.

    g and l are dicts of globals and locals. If provided, l and then g will
    be searched for the class to restore, before attempting to re-import the
    original class module.
    """

    name, module = cls

    if name in l:
        return l[name]

    if name in g:
        return g[name]

    try:
        m = __import__(module, g, l, fromlist=[module])
        if hasattr(m, name):
            return getattr(m, name)
    except ImportError:
        pass

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

        self._key_kids = kids
        self._key_args = args
        self._key_kwargs = kwargs

    return init

def keyable(c):
    """
    Simple class decorator to make a class keyable.
    """

    def keygetter(self):
        return (save_class(c),
            self._key_kids,
            self._key_args,
            self._key_kwargs,
            self.state)

    c.state = None
    c.__init__ = keyinit(c.__init__)
    c.key = property(keygetter)

    return c

def instance_from_key(key):
    """
    Instantiate an object from a key.
    """

    cls, kids, args, kwargs, state = key

    cls = restore_class(cls)

    if kids:
        args = [instance_from_key(i) for i in kids] + list(args)

    inst = cls(*args, **kwargs)
    inst.state = state

    return inst
