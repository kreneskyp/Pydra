import mmap
import os
import os.path

from pydra.cluster.tasks.datasource.slicer import LineSlicer
from pydra.util.key import keyable

@keyable
class DirSelector(object):
    """
    Selects a directory, yielding files.
    """

    def __init__(self, path, recursive=True):
        self.path = path
        if recursive:
            self.files = set()
            for directory, chaff, files in os.walk(self.path):
                self.files.update(
                    os.path.join(directory, i) for i in files)
        else:
            self.files = set(os.path.join(self.path, i)
                for i in next(os.walk(self.path))[2])

    def __iter__(self):
        for f in self.files:
            yield FileSelector(f)

    def __getitem__(self, filename):
        if filename in self.files:
            handle = open(os.path.join(self.path, filename))
            return LineSlicer(handle)
        else:
            raise KeyError

    def __len__(self):
        return len(self.files)

@keyable
class FileSelector(object):
    """
    Selects files. Can yield file-based slicers.
    """

    def __init__(self, path):
        self.path = path

        self._handle = None

    @property
    def handle(self):
        if self._handle:
            return self._handle
        # XXX with h as...?
        # XXX heuristic?
        h = open(self.path, "rb")
        m = mmap.mmap(h.fileno(), 0, prot=mmap.PROT_READ)
        h.close()
        self._handle = m
        return m

@keyable
class SQLSelector(object):
    """
    Selects rows from a SQL database.
    """

    def __init__(self, db):
        if hasattr(db, "handle"):
            self.handle = db.handle
        else:
            self.handle = db
