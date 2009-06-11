
from tasks import Task, TaskNotFoundException, \
    STATUS_RUNNING, STATUS_COMPLETE

from twisted.internet import reactor

import cPickle as pickle
import os, logging

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

        self.im = self.intermediate(msg, self.reducers, **self.intermediate_kwargs)

        self.maptask = MapTask('MapTask', self.map)
        self.maptask.parent = self

        self.reducetask = ReduceTask('ReduceTask', self.reduce)
        self.reducetask.parent = self

    def map(self, input, output, **kwargs):
        pass

    def reduce(self, input, output, **kwargs):
        # default *identity* reduce function
        for k, v in input:
            output[k] = v

    def map_stage_callback(self, result, output=None, mapid=None):
        logger.debug('   map_stage_callback: %s' % mapid)
        self.im.flush(result, adict=output, mapid=mapid)

        try:
            del self.map_tasks[mapid]
        except KeyError:
            logger.debug('   map_stage_callback: no such task -> %s' % mapid)


    def reduce_stage_callback(self, result, output=None, reduceid=None):
        logger.debug('   reduce_stage_callback: %s' % reduceid)
        self.output.update(output)

        try:
            del self.reduce_tasks[reduceid]
        except KeyError:
            logger.debug('   reduce_stage_callback: no such task -> %s' % reduceid)


    def work(self, args, callback, callback_args={}):
        """
        overidden to prevent early cleanup. MapReduceTask doesn not implement _work() and dont expec user to provide its own. Instead it requires user to provade map() and reduce() methods.
        Cleanup is moved to task_complete() wich will be called when there is no mo work remaining.
        """

        self.__callback = callback
        self._callback_args = callback_args

        self._status = STATUS_RUNNING

        worker = self.get_worker()
        logger.debug("mapreduce: workers available: %d" % worker.available_workers)

        # let's start the processing
        self.map_stage()


    def map_stage(self):

        logger.debug('mapreduce: map stage')

        for id, i in enumerate(self.input):

            mout = AppendableDict()

            mapid = 'map%d' % id
            self.map_tasks[mapid] = 1

            logger.debug('   starting maptask: %s' % mapid)
            map_args = {
                        'input': i,
                        'output': mout,
                       }

            if self.sequential:
                result = self.maptask.work(args=map_args)
                self.im.flush(None, adict=mout, mapid=mapid)
                del self.map_tasks[mapid]
            else:
                
                self.maptask.start(args=map_args, callback=self.map_stage_callback,
                                callback_args={'output': mout, 'mapid': mapid, })

                # XXX experiments
                #logger.debug("mapreduce: requesting worker for map-task: %s" % maptask.get_key())
                #self.parent.request_worker(self.maptask.get_key(), {}, id)
                #logger.debug("mapreduce: worker aquired")
                # XXX experiments end

        # call reduce stage
        self.reduce_stage()


    def reduce_stage(self):

        if self.map_tasks:
            logger.debug('mapreduce: waiting for map stage to finish')
            reactor.callLater(1, self.reduce_stage)
            return

        logger.debug('mapreduce: reduce stage')
        for id, i in enumerate(self.im):
            rout = {}

            reduceid = 'reduce%d' % id
            self.reduce_tasks[reduceid] = 1

            logger.debug('   starting reducetask: %s' % reduceid)
            reduce_args = {
                            'input': i,
                            'output': rout,
                          }

            if self.sequential:
                result = self.reducetask.work(args=reduce_args)
                self.output.update(rout)
                del self.reduce_tasks[reduceid]
            else:
                self.reducetask.start(args=reduce_args, callback=self.reduce_stage_callback,
                                    callback_args={'output': rout,'reduceid': reduceid})

        # call final stage
        self.task_complete()


    def task_complete(self):
        """
        Should be called when all map and reduce task have completed
        """

        if self.reduce_tasks:
            logger.debug('mapreduce: waiting for reduce stage to finish')
            reactor.callLater(1, self.task_complete)

        logger.debug('mapreduce: finished')
        logger.info(self.output)

        self._status = STATUS_COMPLETE

        #make a callback, if any
        if self.__callback:
            self.__callback(self.output)


    def get_subtask(self, task_path):
        if len(task_path) == 1:
            if task_path[0] == self.__class__.__name__:
                return self
            else:
                raise TaskNotFoundException("Task not found")

        # pop this classes name off the list
        task_path.pop(0)

        # what if it is MapTask
        try:
            return self.maptask.get_subtask(task_path)
        except TaskNotFoundException:
            pass

        # what if it is ReduceTask
        return self.reducetask.get_subtask(task_path)

    def progress(self):
        return -1


class FunctionTask(Task):

    def __init__(self, msg, fun):
        Task.__init__(self, msg)
        self.msg = msg
        self.fun = fun

    def _work(self, **kwargs):
        return self.fun(**kwargs)


class MapTask(FunctionTask):
    pass


class ReduceTask(FunctionTask):
    pass

