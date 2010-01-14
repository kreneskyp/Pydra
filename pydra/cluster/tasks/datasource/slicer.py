import cStringIO

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

    def __init__(self, handle, sep="\n"):
        if not hasattr(handle, "read"):
            handle = cStringIO.StringIO(handle)
        self.handle = handle
        self.sep = sep

        self.next_sep = self.find_sep(0)

    def next(self):
        self.next_sep = self.find_sep(self.next_sep)
        if self.next_sep is None:
            raise StopIteration
        return self.next_sep

    def find_sep(self, starting_point):
        position = self.handle.tell()
        count = 80
        while True:
            s = self.handle.read(count)
            if not s:
                return None
            index = s.find(self.sep)
            if index != -1:
                position += index
                self.handle.seek(position + 1)
                return position
