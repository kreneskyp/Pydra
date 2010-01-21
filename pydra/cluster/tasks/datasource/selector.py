import mmap
import os
import os.path

from pydra.cluster.tasks.datasource.slicer import LineSlicer

class DirSelector(object):
    """
    Selects files from a directory.
    """

    def __init__(self, path):
        self.path = path
        self.files = set(next(os.walk(self.path))[2])

    def __iter__(self):
        for f in self.files:
            yield os.path.join(self.path, f)

    def __getitem__(self, filename):
        if filename in self.files:
            handle = open(os.path.join(self.path, filename))
            return LineSlicer(handle)
        else:
            raise KeyError

class FileSelector(object):
    """
    Selects files. Can yield file-based slicers.
    """

    def __init__(self, path):
        self.path = path

    @property
    def handle(self):
        # XXX with h as...?
        # XXX heuristic?
        h = open(self.path, "rb")
        m = mmap.mmap(h.fileno(), 0, prot=mmap.PROT_READ)
        h.close()
        return m
