"""
Helper functions for datasource handling.

These are all API functions and may be called by users.

Datasource descriptions are simple tuples of information used to instantiate
datasources. The first member of the tuple is a class, and the other members
are arguments to be given to the class upon instantiation.
"""

from pydra.cluster.tasks.datasource.slicer import IterSlicer

def iterable(i):
    """
    Test for iterability.

    :return: Whether `i` is iterable.
    """

    try:
        iter(i)
        return True
    except TypeError:
        return False

def validate(ds, *args):
    """
    Given a potential datasource description, return its canonical form.

    This function can and will make guesses; it will always return a valid, if
    slightly mangled, datasource description.

    :Parameters:
        ds : tuple
            The datasource description, or some object vaguely similar to a
            datasource description
        args
            Stopgap against TypeError from incorrectly protected tuples. Will
            be appended to the datasource description.
    """

    # If the caller didn't read the docs...
    if args:
        # Hax. Only lists are ordered, mutable, and iterable, and we need all
        # three in order to prepare our tuple. To make matters worse, we need
        # to check the iterability of ds in order to pick the right list
        # constructor (list() vs. []) so that we Do the Right Thing.
        if iterable(ds):
            ds = tuple(list(ds) + list(args))
        else:
            ds = tuple([ds] + list(args))

    # If it's iterable...
    if iterable(ds):
        if not callable(next(iter(ds))):
            # Turn raw iterables into IterSlicers.
            ds = (IterSlicer, ds)
    else:
        # Last-ditch effort: Return a single slice of data.
        ds = (IterSlicer, [ds])

    return ds

def unpack(ds):
    """
    Instantiate and unpack a datasource.
    """

    selector, args = ds[0], ds[1:]

    for s in selector(*args):
        yield s
