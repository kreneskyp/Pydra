from __future__ import with_statement

from threading import Lock

import cPickle as pickle
import os, logging

logger = logging.getLogger('root')


class DatasourceDict(object):

    def __init__(self, dict):
        self.store = dict


    def load(self, key):
        """data reading"""
        # we are the source len(key) == 1: key[0] == key[-1]
        return self.store[key[-1]]


    def __iter__(self):
        """key generation"""
        for key in self.store.iterkeys():
            yield key, # tuple


class SequenceSlicer(object):

    def __init__(self):
        self.input = None


    def load(self, key):
        """data reading"""
        parent, index = key[:-1], key[-1]
        sequence = self.input.load(parent)
        return sequence[index]


    def __iter__(self):
        """key generation"""
        for input_key in self.input:
            seq_len = len(self.input.load(input_key))
            for sequence_key in xrange(seq_len):
                yield input_key + (sequence_key, )

