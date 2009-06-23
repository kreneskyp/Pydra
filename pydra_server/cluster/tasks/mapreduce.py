
from tasks import Task, TaskNotFoundException, \
    STATUS_RUNNING, STATUS_COMPLETE

from twisted.internet import reactor

from threading import Lock

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

        self._lock = Lock()
        self._partitions = {}


    def partition(self, key):
        return hash(str(key)) % self.reducers


    def flush(self, output, mapid):

        logger.debug("im: flushing %s" % str(output))

        pdict = {}

        for k, vs in output.iteritems():

            p = self.partition(k)

            if p in pdict:
                pdict[p].append((k, vs))
            else:
                pdict[p] = [(k, vs)]

        partitions = {}

        for p, tuples in pdict.iteritems():

            filename = self.pattern % (self.task_id, p, mapid)
            partitions[p] = filename

            logger.debug("im: dumping %s to %s" % (str(tuples), filename))
            with open(os.path.join(self.dir, filename), "w") as f:
                for tuple in tuples:
                    pickle.dump(tuple, f)

        return partitions


    def update_partitions(self, partitions):

        with self._lock:
            for p, filename in partitions.items():
                if p in self._partitions:
                    self._partitions[p].append(filename)
                else:
                    self._partitions[p] = [filename]


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
        try:
            with self._lock:
                p, fs = self._partitions.popitem()
        except KeyError:
            raise StopIteration

        return self._partition_iter(fs)


class MapReduceTask(Task):

    input = None
    output = None

    map = None
    reduce = None

    intermediate = IntermediateResultsFiles
    intermediate_kwargs = {'dir': None }

    reducers = 1

    description = "Abstract Map-Reduce Task"

    sequential = False


    def __init__(self, msg=None):
        Task.__init__(self, msg)
        self.map_tasks = {}
        self.reduce_tasks = {}

        self.im = self.intermediate(msg, self.reducers, **self.intermediate_kwargs)

        self.maptask = self.map('MapTask', self.im)
        self.maptask.parent = self

        self.reducetask = self.reduce('ReduceTask')
        self.reducetask.parent = self


    def map_callback(self, result, mapid=None, local=False):
        logger.debug('   map_callback: %s' % mapid)
        logger.debug('   map_callback: %s' % result)
        self.im.update_partitions(result)

        try:
            del self.map_tasks[mapid]
        except KeyError:
            logger.debug('   map_callback: no such task -> %s' % mapid)

        self.map_next(local)


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

        self._reduce_called = False

        # XXX we use current worker
        self._available_workers = self.get_worker().available_workers
        self._input_iter = enumerate(self.input)

        # let's start the processing
        logger.debug('mapreduce: map stage')

        if self._available_workers > 1 and self.sequential is False:
            for i in range(1, self._available_workers):
                self.map_next(local=False)

        self.map_next(local=True)


    def map_next(self, local=False):

        try:
            id, i = self._input_iter.next()
        except StopIteration:
            # call reduce stage
            if not self._reduce_called:
                self._reduce_called = True
                self.reduce_stage()

            return

        mapid = 'map%d' % id
        self.map_tasks[mapid] = 1

        logger.debug('   starting maptask: %s' % mapid)
        map_args = {
                    'id': mapid,
                    'input': i,
                   }

        if self.sequential:
            self.maptask.work(args=map_args, callback=self.map_callback,
                                            callback_args={'mapid': mapid})
        else:
            if local: # XXX orginal worker is to run computations as well, or schedule only?
                logger.debug("mapreduce: running locally maptask: %s" % mapid)
                self.maptask.start(args=map_args, callback=self.map_callback,
                                    callback_args={'mapid': mapid, 'local': local})
            else:
                logger.debug("mapreduce: requesting worker for maptask: %s" % self.maptask.get_key())
                logger.debug("mapreduce: running remotely maptask: %s" % mapid)
                self.parent.request_worker(self.maptask.get_key(), map_args, mapid)
                logger.debug("mapreduce: worker aquired?")


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

            if self.sequential or True: # XXX for now reduce is only sequential
                self.reducetask.work(args=reduce_args, callback=self.reduce_stage_callback,
                                    callback_args={'output': rout, 'reduceid': reduceid})

        # call final stage
        self.task_complete()


    def _work_unit_complete(self, result, id):
        logger.debug("mapreduce: REMOTE got result %s on %s" % (result, id))
        self.map_callback(result, id, local=False)


    def task_complete(self):
        """
        Should be called when all map and reduce task have completed
        """

        if self.reduce_tasks:
            logger.debug('mapreduce: waiting for reduce stage to finish')
            reactor.callLater(1, self.task_complete)
            return

        logger.debug('mapreduce: finished')
        logger.info(self.output)

        self._status = STATUS_COMPLETE

        #make a callback, if any
        if self.__callback:
            self.__callback(self.output, **self._callback_args)


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


class MapReduceSubtask(Task):

    def _generate_key(self):
        if issubclass(self.parent.__class__, (MapReduceTask,)):
            base = self.parent.get_key()
            key = '%s.%s' % (base, self.__class__.__name__)

        else:
            key = Task._generate_key(self)

        return key


class MapTask(MapReduceSubtask):

    def __init__(self, msg, im):
        MapReduceSubtask.__init__(self, msg)
        self.im = im


    def work(self, args={}, callback=None, callback_args={}):
        """
        Overwrites Task.work() beacuse MapTask needs to provide special input and output
        dictionaries. And it is necesairy to flush intermediate results after self._work()
        """
        logger.debug('%s - MapTask - in MapTask.work()'  % self.get_worker().worker_key)
        self._status = STATUS_RUNNING

        output = AppendableDict()
        args['output'] = output

        id = args['id']

        logger.debug("%s._work()" % id)

        self._work(**args) # XXX ignoring results

        results = self.im.flush(output, id) # XXX partitions are our results

        self._status = STATUS_COMPLETE
        logger.debug('%s - MapTask - work complete' % self.get_worker().worker_key)

        self.work_deferred = None

        #make a callback, if any
        if callback:
            logger.debug('%s - MapTask - Making callback' % self)
            callback(results, **callback_args)
        else:
            logger.warning('%s - MapTask - NO CALLBACK TO MAKE: %s' % (self, callback))

        return results


class ReduceTask(MapReduceSubtask):
    pass

