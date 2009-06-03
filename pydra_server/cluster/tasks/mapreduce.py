
from tasks import Task

import cPickle as pickle
import os, logging

logger = logging.getLogger('root')

class IntermediateResultsFiles():
    """this must mimic iterator and dictionary"""
    # mapreduce-i9t-(taks_id)-(partition)
    pattern = "mapreduce-i9t-%s-%d-%s"

    def __init__(self, dir, task_id, reducers=1):
        self.task_id = task_id
        self.reducers = reducers
        self.dir = dir
        self._dict = {}

        # XXX quickfix (storage of partition files)
        self._partitions = {}

    def partition(self, key):
        return hash(str(key)) % self.reducers

    def flush(self, results=None, from_task=None):
        # XXX results is ignored
        logger.debug(" IM.flush() %s" % str(self._dict))

        pdict = {}

        for k, vs in self._dict.iteritems():

            p = self.partition(k)

            if p in pdict:
                pdict[p].append((k, vs))
            else:
                pdict[p] = [(k, vs)]

        for p, tuples in pdict.iteritems():

            filename = self.pattern % (self.task_id, p, from_task)

            # XXX quickfix (storage of partition files)
            if p in self._partitions:
                self._partitions[p].append(filename)
            else:
                self._partitions[p] = [filename]

            logger.debug(" IM.flush(): dumping to %s" % filename)
            with open(os.path.join(self.dir, filename), "w") as f:
                for tuple in tuples:
                    pickle.dump(tuple, f)

        self._dict = {}

    def __setitem__(self, key, value):
        if self._dict.has_key(key):
            self._dict[key].append(value)

        else:
            self._dict[key] = [value]

    def __iter__(self):
        return self

    def _partition_iter(self, files):
        for filename in files:
            logger.debug(" IM.next().next(): loading from %s" % filename)
            with open(os.path.join(self.dir, filename)) as f:
                try:
                    while True:
                        yield pickle.load(f)

                except EOFError:
                    pass

    def next(self):
        # XXX quickfix (storage of partition files)
        try:
            p, fs = self._partitions.popitem()
        except KeyError:
            raise StopIteration

        return self._partition_iter(fs)


class MapReduceTask(Task):
    input = None
    output = None
    im = None

    description = "Abstract Map-Reduce Task"

    def __init__(self, msg=None, sequential=True):
        self.sequential = sequential
        self.subtasks = []

    def map(self, input, output, **kwargs):
        pass

    def reduce(self, input, output, **kwargs):
        # default *identity* reduce function
        for k, v in input:
            output[k] = v

    def _work(self, **kwargs):

        for id, i in enumerate(self.input):
            mapid = 'map%d' % id
            maptask = FunctionTask(mapid, self.map, mapid, i, self.im)
            maptask.parent = self

            logger.debug('   Starting maptask: %s' % maptask)
            if self.sequential:
                result = maptask.work(args=kwargs, callback=self.im.flush,
                                                   callback_args={'from_task': mapid})
            else:
                raise NotImplementedError

        for id, i in enumerate(self.im):
            reduceid = 'reduce%d' % id
            reducetask = FunctionTask(reduceid, self.reduce, reduceid, i, self.output)
            reducetask.parent = self

            logger.debug('   Starting reducetask: %s' % reducetask)
            if self.sequential:
                result = reducetask.work(args=kwargs)
            else:
                raise NotImplementedError

        logger.debug("finished map-reduce, what do you want more?")
        logger.info(self.output)


    def progress(self):
        return -1


class FunctionTask(Task):

    def __init__(self, msg, fun, id, input, output):
        self.fun = fun
        self.input = input
        self.output = output
        self.id = id

        self.msg = msg

    def _work(self, **kwargs):
        return self.fun(self.input, self.output, **kwargs)

