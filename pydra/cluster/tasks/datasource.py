from __future__ import with_statement

from threading import Lock

import cPickle as pickle
import UserDict
import logging
import os

import MySQLdb

logger = logging.getLogger('root')

def chain_subslicer(obj, ss_list):

    last_ss = obj
    for ss in ss_list:
        ss.input = last_ss
        ss.send_as_input = True
        last_ss = ss

    return last_ss

############
# sources

class Datasource(object, UserDict.DictMixin):

    def __init__(self):
        self.subslicer = None

    def connect(self):
        pass

    def close(self):
        pass

    def __iter__(self):
        # implement this
        raise NotImplementedError

    def _load(self, key):
        # implement this
        raise NotImplementedError

    def load(self, key):
        obj = self._load(key)

        if self.subslicer:
            return chain_subslicer(obj, self.subslicer)

        return obj


class DatasourceDict(Datasource):

    def __init__(self, dictionary):
        super(DatasourceDict, self).__init__()
        self.store = dictionary


    def __iter__(self):
        """key generation"""
        for key in self.store.iterkeys():
            yield key, # tuple

    def _load(self, key):
        """data reading"""

        return self.store[key[-1]]


class DatasourceDir(Datasource):

    def __init__(self, directory):
        super(DatasourceDir, self).__init__()
        self.directory = directory


    def __iter__(self):
        """generate key for input files"""
        files = iter(os.walk(self.directory)).next()[2]

        for filename in files:
            yield filename,

    def _load(self, key, mode="r"):
        """open particular input file"""
        filename = key[-1]
        path = os.path.join(self.directory, filename)
        return open(path, mode)


class DatasourceSQL(Datasource):

    def __init__(self, **kwargs):
        super(DatasourceSQL, self).__init__()
        self.kwargs = kwargs
        self.db = None

    def connect(self):
        self.db = MySQLdb.connect(**self.kwargs)
        logger.debug("datasource: connecting to DB")

    def close(self):
        self.db.close()
        logger.debug("datasource: closing connection to DB")


    def __iter__(self):
        """generate key for database"""
        yield "_mysql",

    def _load(self, key):
        return self.db.cursor()

