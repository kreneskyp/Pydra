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
    """
    The parent of all data sources, and the reference implementation of the
    Datasource API.
    """

    def __init__(self):
        self.subslicer = None

    def __getitem__(self, key):
        obj = self.load(key)

        if self.subslicer:
            return chain_subslicer(obj, self.subslicer)

        return obj

    def __iter__(self):
        """
        Return an iterator of keys.

        Each key must be returned as a 1-tuple.
        """
        raise NotImplementedError

    def open(self):
        logger.debug("%s: Stub: Opening..." % self.__class__.__name__)

    def close(self):
        logger.debug("%s: Stub: Closing..." % self.__class__.__name__)

    def load(self, key):
        """
        Called internally to look up the data corresponding to `key`.
        Must be implemented by subclasses.
        """
        raise NotImplementedError


class DatasourceList(Datasource):
    """
    A simple datasource that wraps a list.
    """

    def __init__(self, l):
        super(DatasourceList, self).__init__()
        self.list = l

    def __iter__(self):
        return ((index,) for index in xrange(len(self.list)))

    def load(self, key):
        return self.list[key[-1]]


class DatasourceDict(Datasource):
    """
    A simple datasource that wraps a dict.
    """

    def __init__(self, dictionary):
        super(DatasourceDict, self).__init__()
        self.store = dictionary

    def __iter__(self):
        return ((key,) for key in self.store.iterkeys())

    def load(self, key):
        return self.store[key[-1]]


class DatasourceDir(Datasource):

    def __init__(self, directory):
        super(DatasourceDir, self).__init__()
        self.directory = directory

    def __iter__(self):
        """generate key for input files"""
        files = next(os.walk(self.directory))[2]
        return ((filename,) for filename in files)

    def load(self, key, mode="r"):
        """open particular input file"""
        filename = key[-1]
        path = os.path.join(self.directory, filename)
        return open(path, mode)


class DatasourceSQL(Datasource):

    def __init__(self, **kwargs):
        super(DatasourceSQL, self).__init__()
        self.kwargs = kwargs
        self.db = None

    def open(self):
        self.db = MySQLdb.connect(**self.kwargs)
        logger.debug("datasource: connecting to DB")

    def close(self):
        self.db.close()
        logger.debug("datasource: closing connection to DB")

    def __iter__(self):
        """generate key for database"""
        yield "_mysql",

    def load(self, key):
        return self.db.cursor()

