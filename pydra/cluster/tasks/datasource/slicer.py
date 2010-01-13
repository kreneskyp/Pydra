class IterSlicer(object):
    """
    Slicer that operates on iterables.
    """

    def __init__(self, iterable):
        self.iterable = iterable
        self.iterator = iter(iterable)

    def __iter__(self):
        return self

    def next(self):
        return next(self.iterator)

class MapSlicer(IterSlicer):
    """
    Slicer that operates on mappings.

    Mappings should implement the standard interface.
    """

    pass

class LineSlicer(IterSlicer):
    """
    Slicer specialized for handling text blobs.
    """

    def __init__(self, blob, sep="\n"):
        self.blob = blob
        self.sep = sep
        self.iterator = (i for i in blob.split(sep))
