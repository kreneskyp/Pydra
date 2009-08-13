from __future__ import with_statement

from threading import Lock

import cPickle as pickle
import os, logging

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

class DatasourceDict(object):

    def __init__(self, dict):
        self.store = dict
        self.subslicer = None

    def connect(self):
        pass

    def close(self):
        pass


    def __iter__(self):
        """key generation"""
        for key in self.store.iterkeys():
            yield key, # tuple


    def load(self, key):
        """data reading"""
        # we are the source len(key) == 1: key[0] == key[-1]

        obj = self.store[key[-1]]

        if self.subslicer:
            return chain_subslicer(obj, self.subslicer)

        return obj


class DatasourceDir(object):

    def __init__(self, dir):
        self.dir = dir
        self.subslicer = None

    def connect(self):
        pass

    def close(self):
        pass


    def __iter__(self):
        """generate key for input files"""
        files = os.walk(self.dir).__iter__().next()[2]

        for filename in files:
            yield filename,


    def load(self, key):
        """open particular input file"""
        filename = key[-1]
        path = os.path.join(self.dir, filename)
        return open(path)


class DatasourceSQL(object):

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.subslicer = None
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

    def load(self, key):
        obj = self.db.cursor()

        if self.subslicer:
            return chain_subslicer(obj, self.subslicer)

        return obj


############
# slicers

class Slicer(object):

    def __init__(self, **kwargs):
        self.input = None
        self.kwargs = kwargs

        self.send_as_input = False
        self.subslicer = None


    def __iter__(self):
        """iterator generating keys"""
        # implement this
        raise NotImplementedError


    def load(self, key):
        """loading data corresponding with particular key"""

        #if self.send_as_input then the key is the input
        if self.send_as_input:
            return key

        obj = self._load(key)

        if self.subslicer:
            return chain_subslicer(obj, self.subslicer)

        return obj


    def _load(self, key):
        """loading data corresponding with particular key"""
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
        """generates a key == parent_key + (offset, ), where offset is a line position in file"""
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
        """reads particular line in file"""
        parent, offset = key[:-1], key[-1]
        with self.input.load(parent) as f:
            f.seek(offset)
            line = f.readline().strip()

        return line


class SQLTableSlicer(Slicer):

    def __iter__(self):
        for input_key in self.input:
            c = self.input.load(input_key)

            c.execute("SELECT id FROM %s" % self.kwargs['table'])
            row = c.fetchone()

            while row:
                id = int(row[0])
                if self.send_as_input:
                    yield id

                else:
                    yield input_key + (id, )

                row = c.fetchone()


    def _load(self, key):
        """reads particular row in table"""
        parent, id = key[:-1], key[-1]
        c = self.input.load(parent)

        c.execute("SELECT * FROM %s WHERE id = %d" % (self.kwargs['table'], id))
        return c.fetchone()


############
# subslicers

class Subslicer(Slicer):

    def __init__(self, **kwargs):
        super(Subslicer, self).__init__(**kwargs)
        self.send_as_input = True


class FileUnpicleSubslicer(Subslicer):

    def __iter__(self):
        dir = self.kwargs['dir']

        for filename in self.input:

            try:
                with open(os.path.join(dir, filename)) as f:
                    while True:
                        yield pickle.load(f)

            except EOFError:
                logger.debug("subslicer: loading from %s done" % f.name)
                pass


class FilePickleOutput(object):

    def __init__(self, dir):
        self.dir = dir

    def dump(self, key, values):
        with open(os.path.join(self.dir, key), "w") as f:
            for obj in values:
                pickle.dump(obj, f)


class SQLTableKeyInput(Subslicer):

    def __iter__(self):

        db = self.kwargs['db']
        table = self.kwargs['table']
        c = db.load(None)

        for partition in self.input:

            sql = "SELECT k, v FROM %s WHERE partition = '%s'" % (table, partition)
            logger.debug(sql)

            c.execute(sql)
            row = c.fetchone()

            while row:
                yield row

                row = c.fetchone()


class SQLTableOutput(object):

    def __init__(self, db, table, **kwargs):
        self.table = table
        self.db = db
        self.kwargs = kwargs


    def dump(self, key, tuples):
        c = self.db.load(None)
        for tuple in tuples:
            k, vals = tuple

            try:
                vals = iter(vals)
            except TypeError:
                vals = [vals]

            for v in vals:
                sql = "INSERT INTO %s (partition, k, v) VALUES ('%s', '%s', '%s')" % \
                        (self.table, key, k, str(v))
                logger.debug(sql)

                r = c.execute(sql)
                logger.debug("inserted %s" % r)


