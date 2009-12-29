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
        select_args = {'id_column': 'id'}
        select_args.update(self.kwargs)

        select_query = select_args.pop('select_query',
                "SELECT %(id_column)s FROM %(table)s")

        for input_key in self.input:
            c = self.input.load(input_key)

            c.execute(select_query % select_args)
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

        select_args = {'id_column': 'id','id': id}
        select_args.update(self.kwargs)

        load_query = select_args.pop('load_query',
                "SELECT * FROM %(table)s WHERE %(id_column)s = %(id)d")

        c.execute(load_query % select_args)
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
                with dir.load((filename, )) as f:
                    while True:
                        yield pickle.load(f)

            except EOFError:
                logger.debug("subslicer: loading from %s done" % f.name)
                pass


class FilePickleOutput(object):

    def __init__(self, dir):
        self.dir = dir

    def dump(self, key, values):
        with self.dir._load((key, ), mode="w") as f:
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

    def __init__(self, db, table):
        self.table = table
        self.db = db


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


