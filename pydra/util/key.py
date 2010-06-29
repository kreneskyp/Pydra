"""
Tools for automatic serialization and deserialization.

This module introduces two concepts: Keys and state. A class may be defined
as "keyable," meaning that its instances will have an attribute called "key,"
which may be used to recreate that instance at another run time, e.g. after
pickling and thawing.

Keys always have the following format:

(cls, children, args, kwargs, state)

~ cls is an identifier built from primitives that can be safely pickled.
~ children is a list of keyable args; see below.
~ args is the list of positional arguments given to __init__.
~ kwargs is the dict of keyword arguments given to __init__.
~ state is a class-definable primitive; see below.

If any of the original args of the instance are keyable, they will be
serialized in the key as well, in the children list. In order for this to
work, there is currently a restriction, as follows: Keyable args must come
before any non-keyable args if they are to be serialized.

If a class stores internal state in its instance, and wishes for that state
to be restored automatically upon reinstantiation, it may make available an
attribute called "state," which will automatically be saved and restored.
(For ease of use, it is highly recommended to use a property for the state
attribute.)

~ C.
"""

import functools

def save_class(cls):
    """
    Serialize a class handle.

    Not intended for public use.
    """

    return cls.__name__, cls.__module__

def restore_class(cls, g={}, l={}):
    """
    Deserialize a class identifier.

    g and l are dicts of globals and locals. If provided, l and then g will
    be searched for the class to restore, before attempting to re-import the
    original class module.

    Returns the class on success, or None on failure.

    Not intended for public use.
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

def keyable_init(f):
    """
    Simple decorator to be placed on a class's __init__. Instances of the
    class will have a key attribute, which can be used to serialize and
    deseralize the class.

    Keyable classes can be nested inside other keyable classes and will still
    serialize properly.

    Not intended for public use; when possible, use @keyable on your classes
    instead of using this decorator directly.
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

    if not hasattr(c, "state"):
        c.state = None

    c.__init__ = keyable_init(c.__init__)
    c.key = property(keygetter)

    return c

def thaw(key):
    """
    Instantiate an object from a key.

    Returns None if the key could not be used.
    """

    cls, kids, args, kwargs, state = key

    cls = restore_class(cls)
    if not cls:
        return None

    if kids:
        args = [thaw(i) for i in kids] + list(args)

    inst = cls(*args, **kwargs)
    inst.state = state

    return inst
