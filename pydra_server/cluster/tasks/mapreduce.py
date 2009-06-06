
from tasks import Task

import cPickle as pickle
import os, logging, time

logger = logging.getLogger('root')

class AppendableDict(dict):

    def __setitem__(self, key, value):
        if not self.has_key(key):
            super(AppendableDict, self).__setitem__(key, [])
        
        super(AppendableDict, self).__getitem__(key).append(value)


class IntermediateResultsFiles():
    """this must mimic iterator and dictionary"""
    # mapreduce-i9t-(taks_id)-(partition)
    pattern = "mapreduce-i9t-%s-%d-%s"

    def __init__(self, task_id, reducers, dir=None):
        self.task_id = task_id
        self.reducers = reducers
        self.dir = dir

        # XXX quickfix (storage of partition files)
        self._partitions = {}

    def partition(self, key):
        return hash(str(key)) % self.reducers

    def flush(self, result=None, adict=None, mapid=None):

        logger.debug("im: flushing %s" % str(adict))

        pdict = {}

        for k, vs in adict.iteritems():

            p = self.partition(k)

            if p in pdict:
                pdict[p].append((k, vs))
            else:
                pdict[p] = [(k, vs)]

        for p, tuples in pdict.iteritems():

            filename = self.pattern % (self.task_id, p, mapid)

            # XXX quickfix (storage of partition files)
            if p in self._partitions:
                self._partitions[p].append(filename)
            else:
                self._partitions[p] = [filename]

            logger.debug("im: dumping %s to %s" % (str(tuples), filename))
            with open(os.path.join(self.dir, filename), "w") as f:
                for tuple in tuples:
                    pickle.dump(tuple, f)


    def __iter__(self):
        return self

    def _partition_iter(self, files):
        for filename in files:
            logger.debug("im: loading from %s" % filename)
            with open(os.path.join(self.dir, filename)) as f:
                try:
                    while True:
                        yield pickle.load(f)

                except EOFError:
                    logger.debug("im: loading from %s done" % filename)
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

    intermediate = IntermediateResultsFiles
    intermediate_kwargs = {'dir': None }

    reducers = 1

    description = "Abstract Map-Reduce Task"

    sequential = True

    def __init__(self, msg=None):
        Task.__init__(self, msg)
        self.map_tasks = {}
        self.reduce_tasks = {}
        self.im = None

    def map(self, input, output, **kwargs):
        pass

    def reduce(self, input, output, **kwargs):
        # default *identity* reduce function
        for k, v in input:
            output[k] = v

    def map_stage_callback(self, result, adict=None, mapid=None):
        logger.debug('   map_stage_callback: %s' % mapid)
        self.im.flush(result, adict=adict, mapid=mapid)

        try:
            del self.map_tasks[mapid]
        except KeyError:
            logger.debug('   map_stage_callback: no such task -> %s' % mapid)


    def reduce_stage_callback(self, result, reduceid=None):
        logger.debug('   reduce_stage_callback: %s' % reduceid)

        try:
            del self.reduce_tasks[reduceid]
        except KeyError:
            logger.debug('   reduce_stage_callback: no such task -> %s' % reduceid)


    def _work(self, **kwargs):

        self.im = im = self.intermediate(self.msg, self.reducers, **self.intermediate_kwargs)
        logger.debug('mapreduce: map stage')

        for id, i in enumerate(self.input):

            mout = AppendableDict()

            mapid = 'map%d' % id
            maptask = FunctionTask(mapid, self.map, mapid, i, mout)
            maptask.parent = self

            self.map_tasks[mapid] = maptask

            logger.debug('   starting maptask: %s' % mapid)
            if self.sequential:
                result = maptask.work(args=kwargs)
                im.flush(None, adict=mout, mapid=mapid)
                del self.map_tasks[mapid]
            else:
                maptask.start(args=kwargs, callback=self.map_stage_callback,
                                callback_args={'adict': mout, 'mapid': mapid, })

        while self.map_tasks:
            logger.debug('mapreduce: waiting for map stage to finish')
            time.sleep(1)

        logger.debug('mapreduce: reduce stage')
        for id, i in enumerate(im):
            reduceid = 'reduce%d' % id
            reducetask = FunctionTask(reduceid, self.reduce, reduceid, i, self.output)
            reducetask.parent = self

            self.reduce_tasks[reduceid] = reducetask

            logger.debug('   starting reducetask: %s' % reduceid)
            if self.sequential:
                result = reducetask.work(args=kwargs)
                del self.reduce_tasks[reduceid]
            else:
                reducetask.start(args=kwargs, callback=self.reduce_stage_callback,
                                            callback_args={'reduceid': reduceid})

        while self.reduce_tasks:
            logger.debug('mapreduce: waiting for reduce stage to finish')
            time.sleep(1)

        logger.debug('mapreduce: finished')
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

