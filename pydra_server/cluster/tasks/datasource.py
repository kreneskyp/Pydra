from __future__ import with_statement

from threading import Lock

import cPickle as pickle
import os, logging

logger = logging.getLogger('root')


class DatasourceDict(object):

    def __init__(self, dict):
        self.store = dict


    def __iter__(self):
        """key generation"""
        for key in self.store.iterkeys():
            yield key, # tuple

    def load(self, key):
        """data reading"""
        # we are the source len(key) == 1: key[0] == key[-1]
        return self.store[key[-1]]


class DatasourceDir(object):

    def __init__(self, dir):
        self.dir = dir

    def __iter__(self):
        """generate key to files"""
        files = os.walk(self.dir).__iter__().next()[2]

        for filename in files:
            yield filename,


    def load(self, key):
        filename = key[-1]
        path = os.path.join(self.dir, filename)
        return open(path)


class Slicer(object):

    def __init__(self):
        self.input = None
        self.send_as_input = False


    def __iter__(self):
        # implement this
        raise NotImplementedError


    def load(self, key):
        if self.send_as_input:
            return key

        return self._load(key)


    def _load(self, key):
        # implement this
        raise NotImplementedError


class SequenceSlicer(Slicer):

    def __iter__(self):
        """key generation"""
        for input_key in self.input:
            seq = self.input.load(input_key)
            if self.send_as_input:
                for input in seq:
                    yield input
            else:
                for index in xrange(len(seq)):
                    yield input_key + (index, )


    def _load(self, key):
        """data reading"""
        parent, index = key[:-1], key[-1]
        sequence = self.input.load(parent)
        return sequence[index]


class LineFileSlicer(Slicer):

    def __iter__(self):
        for input_key in self.input:
            with self.input.load(input_key) as f:
                #must use readline:
                # for line in f: consumes whole file and
                # f.tell() become useless
                offset = 0
                line = f.readline()

                while line:
                    if self.send_as_input:
                        yield line.strip()
                    else:
                        yield input_key + (offset, )

                    offset = f.tell()
                    line = f.readline()


    def _load(self, key):
        parent, offset = key[:-1], key[-1]
        with self.input.load(parent) as f:
            f.seek(offset)
            line = f.readline().strip()

        return line

