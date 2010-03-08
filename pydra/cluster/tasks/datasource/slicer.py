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

        self.handle.seek(0)
        self._state = self.find_sep()

    def next(self):
        self._state = self.find_sep()
        if self._state is None:
            raise StopIteration
        return self._state

    def find_sep(self):
        position = self.handle.tell()
        count = 80
        s = ""
        temp = self.handle.read(count)
        while temp:
            s += temp
            index = s.find(self.sep)
            if index != -1:
                # Update file handle
                self.handle.seek(position + index + 1)
                # Update approximate count
                count = mma(count, index, 100)

                return position + index
            temp = self.handle.read(count)
        # EOF
        return None

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value
        self.handle.seek(value + 1)
