import cStringIO

from pydra.cluster.tasks.datasource.key import keyable

@keyable
class IterSlicer(object):
    """
    Slicer that operates on iterables.
    """

    def __init__(self, iterable):
        self.iterable = iterable
        self.state = iter(iterable)

    def __iter__(self):
        return self

    def next(self):
        return next(self.state)

@keyable
class MapSlicer(IterSlicer):
    """
    Slicer that operates on mappings.

    Mappings should implement the standard interface.
    """

    pass

def mma(old, new, weight):
    """
    Performs a Moving Modified Average, using the old value, new value,
    and a weight.

    Weight must be greater than zero.

    Borrowed from public-domain code by Corbin Simpson
    http://github.com/MostAwesomeDude/madsnippets
    """

    return ((weight - 1) * old + new) / weight


@keyable
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
                # EOF
                return None
            index = s.find(self.sep)
            if index != -1:
                # Update file handle
                position += index
                self.handle.seek(position + 1)
                # Update approximate count
                count = mma(count, index, 100)

                return position
