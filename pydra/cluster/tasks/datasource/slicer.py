import cStringIO

from pydra.util.key import keyable

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

        self._endpos = None

        self.handle.seek(0)
        self._state = self.find_sep()

    def __getitem__(self, key):
        if not isinstance(key, slice):
            raise TypeError
        # Hack together a copy of ourselves, set its state, and then expand
        # the final slice to cover the last sep in the handle
        ls = LineSlicer(self.handle, self.sep)
        start, stop = key.start, key.stop
        if start > stop:
            start, stop = stop, start
        ls.state = stop
        stop = ls.find_sep()
        if stop:
            ls.state = slice(start, stop)
        else:
            ls.state = start
        return ls

    def next(self):
        self._state = self.find_sep()
        if self._state is None or (self._endpos and self._state > self._endpos):
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
        if self._endpos:
            return slice(self._state, self._endpos)
        else:
            return self._state

    @state.setter
    def state(self, value):
        if isinstance(value, slice):
            self._state, self._endpos = value.start, value.stop
        else:
            self._state = value
        self.handle.seek(self._state + 1)
