"""
Helper functions for datasource handling.

These are all API functions and may be called by users.

Datasource descriptions are simple tuples of information used to instantiate
datasources. The first member of the tuple is a class, and the other members
are arguments to be given to the class upon instantiation.
"""

from pydra.cluster.tasks.datasource.slicer import IterSlicer

def validate(ds):
    """
    Given a potential datasource description, return its canonical form.

    This function can and will make guesses; it will always return a valid, if
    slightly mangled, datasource description.
    """

    # If it's iterable...
    if hasattr(ds, "__iter__"):
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
