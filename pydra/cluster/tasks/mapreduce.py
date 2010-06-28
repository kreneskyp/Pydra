from __future__ import with_statement

from threading import Lock
import cPickle as pickle
import logging
import os

from twisted.internet import reactor, threads

from tasks import Task, TaskNotFoundException, \
    STATUS_RUNNING, STATUS_COMPLETE
from pydra.cluster.tasks.datasource import *

logger = logging.getLogger('root')


class AppendableDict(dict):
    """Extended dictionary which can hold multiple values within one key.

    Values are kept in a list.

    >>> a = AppendableDict()
    >>> a['key'] = 1
    >>> a
    {'key': [1]}
    >>> a['key'] = 2
    >>> a
    {'key': [1, 2]} 
    """

    def __setitem__(self, key, value):
        if not self.has_key(key):
            super(AppendableDict, self).__setitem__(key, [])

        super(AppendableDict, self).__getitem__(key).append(value)


class IntermediateResults(object):
    """Datahandler for not direct input/output handling.

    Implements partitioning part. Backend writing/reading needs
    to be implemented in subclass.

    map stage:
    * map task output is a dictionary;
    * when map._work() is completed output dict is partitioned and dumped;
    * partition_output() partitions items depending on a partition() function;
    * dump() dumps them into a unique file, returns partition-dictionary;
    * every map task's dump partition-dictionary is collected and provided
      to update_partitions() function for future iterator generation.

    partition:
    * number of partitions equals number of reducers;
    * partition must assure that a specific key will be processed by
      one and only one reduce task.

    reduce stage:
    * __iter__() returns an iterator, which generates input keys for each
      partition;
    * load() returns iterator which is used as a input iterator
      for a reduce task (subslicers);
    * iterator loads (key, values) tuples from a backend.
    """

    # mapreduce-i9e-(taks_id)-(partition)-(map_id)
    pattern = "mapreduce-i9e-%s-%d-%s"


    def __init__(self):
        self.task_id = "mapreduce_task"
        self.reducers = 1

        self._partitions = {}

        self.map_output = None
        self.reduce_input = None

    def clear(self):
        self._partitions.clear()


    def partition(self, key):
        """partition key depending on a number of a reducers"""
        return hash(str(key)) % self.reducers


    def partition_output(self, output):
        """iterates through an output dictionary and partitions it,
        returns a dictionary where key is a partition number and value - items
        of output dict belonging to this partition"""

        pdict = {}

        for k, vs in output.iteritems():

            p = self.partition(k)

            if p in pdict:
                pdict[p].append((k, vs))
            else:
                pdict[p] = [(k, vs)]

        return pdict.iteritems()


    def update_partitions(self, partitions):
        """updates partition-dictionary for future iterator generation."""

        for p, filename in partitions.items():
            if p in self._partitions:
                self._partitions[p].append(filename)
            else:
                self._partitions[p] = [filename]


    def __iter__(self):
        return self._partitions.itervalues()


    def load(self, key):
        self.reduce_input.input = key
        return self.reduce_input


    def dump(self, pdict, mapid):
        """dumps a dictionary to a backend.
        returns corresponding partitions-dictionary"""

        logger.debug("im: dumping %s" % str(pdict))

        partitions = {}

        for p, tuples in pdict:

            key = self.pattern % (self.task_id, p, mapid)
            partitions[p] = key

            logger.debug("im: dumping %s to %s" % (str(tuples), key))

            self.map_output.dump(key, tuples)

        return partitions


class IntermediateResultsFiles(IntermediateResults):
    """Storing intermediate results in flat files."""

    def __init__(self, dir):
        super(IntermediateResultsFiles, self).__init__()
        self.dir = dir

        self.map_output = FilePickleOutput(dir=dir)
        self.reduce_input = FileUnpicleSubslicer(dir=dir)


class IntermediateResultsSQL(IntermediateResults):
    """Storing intermediate results in SQL table."""

    def __init__(self, table, db):
        super(IntermediateResultsSQL, self).__init__()
        self.table = table

        self.map_output = SQLTableOutput(db=db, table=table)
        self.reduce_input = SQLTableKeyInput(db=db, table=table)


class MapReduceTask(Task):

    datasources = {}

    input = None
    output = None

    map = None
    reduce = None

    reducers = 1

    description = "Abstract Map-Reduce Task"

    sequential = False


    def __init__(self, msg=None):
        Task.__init__(self, msg)
        self.__lock = Lock()
        self.map_tasks = {}
        self.reduce_tasks = {}

        self.im = self.intermediate
        self.im.task_id = msg
        self.im.reducers = self.reducers

        self.maptask = MapWrapper(self.map('MapTask'), self.im, self)

        self.reducetask = ReduceWrapper(self.reduce('ReduceTask'), self.im,self)

        for src in self.datasources.itervalues():
            src.open()


    def _start(self, args, callback, callback_args={}):
        """overridden to prevent early cleanup.
        
        MapReduceTask does not implement work() and doesn't expect user to
        provide its own. Instead it requires user to provide map and reduce
        attributes which should be classes derived from MapTask and ReduceTask
        respectively.

        Cleanup is in _complete() which will be called when there is no more
        work remaining.
        map:
        * work(): initialization and calling map_next() for every worker
          available;
        * map_next(): checking if any data to process and starting a map task,
          if no more data available, call reduce_stage();
        
        reduce:
        * reduce_next(): checking if any data to process and starting a reduce
          task, if no more data available, call _complete();

        _complete:
        * cleanup and callbacks.
        """

        self.__callback = callback
        self._callback_args = callback_args

        self._status = STATUS_RUNNING
        self._reduce_called = False
        self._input_iter = enumerate(self.input)

        # let's start the processing
        logger.debug('mapreduce: map stage')

        self.request_work()

    def request_work(self):
        """
        Sends work requests to the master.  This function will send either
        Map requests or Reduce requests depending on what work remains.
        """
        if not self._reduce_called:
            while self.map_next():
                pass

        if not self.map_tasks:
            if not self._reduce_called:
                self._partition_iter = enumerate(self.im)
                self._reduce_called = True

            while self.reduce_next():
                pass


    def map_next(self):
        """more work for a map task"""
        try:
            id, i = self._input_iter.next()
        except StopIteration:
            return False

        mapid = 'map%d' % id
        self.map_tasks[mapid] = 1
        map_args = {
                    'id': mapid,
                    'input_key': i,
                   }

        logger.debug("mapreduce: requesting worker for %s: %s"
                % (mapid, self.maptask.get_key()) )
        self.parent.request_worker(self.maptask.get_key(), map_args, mapid)

        return True


    def reduce_next(self):
        """more work for reduce task"""

        try:
            id, p = self._partition_iter.next()
        except StopIteration:
            return False

        reduceid = 'reduce%d' % id
        self.reduce_tasks[reduceid] = 1
        reduce_args = {
                        'partition': p,
                      }

        logger.debug("mapreduce: requesting worker for %s: %s"
                % (reduceid, self.reducetask.get_key()) )
        self.parent.request_worker(self.reducetask.get_key(), reduce_args, \
                                   reduceid)

        return True


    def _work_unit_complete(self, result, id):
        """retrieving results form remote task"""

        logger.debug("mapreduce: got REMOTE result %s from %s" % (result, id))
        with self.__lock:
            # map/reduce specific post processing
            if id in self.map_tasks:
                logger.debug('   map result %s: %s' % (id, result))
                self.im.update_partitions(result)
                del self.map_tasks[id]

            elif id in self.reduce_tasks:
                logger.debug('   reduce result %s: %s' % (id, result))
                self.output.update(result)
                del self.reduce_tasks[id]

            # call request work to ensure that any additional work gets
            # requested before the task completion checks
            self.request_work()

            # XXX figure out how to determine when to release unneeded workers
            if False:
                logger.debug('[%s] MapReduceTask - releasing a worker' % \
                        self.get_worker().worker_key)
                self.get_worker().request_worker_release()

            if not self.map_tasks and not self.reduce_tasks:
                # all work is done, call the task specific function to combine
                # the results
                self._complete()
                return


    def _complete(self):
        """
        Should be called when all map and reduce task have completed
        """

        if self.reduce_tasks:
            logger.debug('mapreduce: waiting for reduce stage to finish')
            reactor.callLater(1, self._complete)
            return

        self.im.clear()

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


class MapReduceWrapper():
    """map-reduce wrapper base class.

    It expects to work() to do some special stuff before and after subtask
    (self.task) real work() method.

    It stores intermediate results helper (self.im) and overrides:
    * get_key() to assure proper subtask identification,
    * get_subtask() to return self instead of subtask directly,
    * start() to run special self.work() instead of subtask's"""

    def __init__(self, task, im, parent):
        self.task = task
        self.im = im
        self.parent = parent
        self.task.parent = parent


    def get_key(self):
        return self.task.get_key()


    def get_worker(self):
        return self.parent.get_worker()


    def get_subtask(self, task_path):
        """It is pretending it's the wrapped task."""

        # this is a wrapper
        if len(task_path) == 1 and task_path[0] == self.task.__class__.__name__:
            return self

        # not looking for the task we are wrapping
        return self.task.get_subtask(task_path)


    def __repr__(self):
        return self.task.__repr__()


    def start(self, args={}, subtask_key=None, callback=None, callback_args={},\
              errback=None):
        """
        starts the task.  This will spawn the work in a workunit thread.
        """

        # only start if not already running
        if self.task._status == STATUS_RUNNING:
            return

        logger.debug('MapReduceWrapper - starting task: %s' % args)
        self.work_deferred = threads.deferToThread(self._start, args, callback,\
                                                    callback_args)

        if errback:
            self.work_deferred.addErrback(errback)

        return 1


class MapWrapper(MapReduceWrapper):

    def _start(self, args={}, callback=None, callback_args={}):
        """
        Overwrites Task._start() because MapTask needs to provide special input
        and output dictionaries. And it is necessary to self.im.dump()
        intermediate results after self.work().
        """
        logger.debug('%s - MapWrapper.work()'  % self.get_worker().worker_key)

        if args.has_key('input_key') and hasattr(self.parent, 'input'):
            args['input'] = self.parent.input.load(args['input_key'])

        output = AppendableDict()
        args['output'] = output

        id = args['id']
        logger.debug("%s._work()" % id)

        self.task._work(**args) # ignoring results

        pdict = self.im.partition_output(output)

        logger.debug("%s._work() dumping i9e" % id)
        results = self.im.dump(pdict, id) # partitions are our results

        logger.debug('%s - MapWrapper - work complete' % \
                     self.get_worker().worker_key)

        #make a callback, if any
        if callback:
            logger.debug('%s - MapWrapper - Making callback' % self)
            callback(results, **callback_args)
        else:
            logger.warning('%s - MapWrapper - NO CALLBACK TO MAKE: %s' % \
                           (self, callback))

        return results


class ReduceWrapper(MapReduceWrapper):

    def _start(self, args={}, callback=None, callback_args={}):
        """
        Overwrites Task._start() beacuse ReduceTask needs to provide special
        input dictionaries (from self.im).
        """
        logger.debug('%s - ReduceWrapper.work()' % self.get_worker().worker_key)

        args['input'] = self.im.load(args['partition'])
        output = args['output'] = {}

        self.task._work(**args) # ignoring results
        results = output

        logger.debug('%s - ReduceWrapper - work complete' % \
                     self.get_worker().worker_key)

        #make a callback, if any
        if callback:
            logger.debug('%s - ReduceWrapper - Making callback' % self)
            callback(results, **callback_args)
        else:
            logger.warning('%s - ReduceWrapper - NO CALLBACK TO MAKE: %s' % \
                           (self, callback))

        return results

