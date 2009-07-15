from __future__ import with_statement

from tasks import Task, TaskNotFoundException, \
    STATUS_RUNNING, STATUS_COMPLETE

from twisted.internet import reactor, threads

from threading import Lock

import cPickle as pickle
import os, logging

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


class IntermediateResultsFiles():
    """Storing intermediate results in flat files.

    map stage:
    * map task output is a dictionary (which must be pick-able);
    * when map.work() is completed output dictionary is flushed;
    * flush() partitions items depending on a partition() function
      and dumps them into a unique file, returns partition-dictionary;
    * every map task flush partition-dictionary is collected and provided
      to update_partitions() function for future iterator generation.

    partition:
    * number of partitions equals number of reducers;
    * partition must assure that a specific key will be processed by
      one and only one reduce task.

    reduce stage:
    * next() method returns list of all files within one partition;
    * list of files is provided to _partition_iter() for every reduce task;
    * _partition_iter() returns iterator which is used as a input iterator
      for a reduce task;
    * iterator loads from (key, values) tuples from a files.
    """

    # mapreduce-i9t-(taks_id)-(partition)
    pattern = "mapreduce-i9t-%s-%d-%s"


    def __init__(self, task_id, reducers, dir=None):
        self.task_id = task_id
        self.reducers = reducers
        self.dir = dir

        self._lock = Lock()
        self._partitions = {}


    def partition(self, key):
        """partition key depending on a number of a reducers"""
        return hash(str(key)) % self.reducers


    def partition_output(self, output):
        pdict = {}

        for k, vs in output.iteritems():

            p = self.partition(k)

            if p in pdict:
                pdict[p].append((k, vs))
            else:
                pdict[p] = [(k, vs)]

        return pdict.iteritems()


    def flush(self, partition, mapid):
        """dumps a dictionary to a files.
        returns corresponding partitions-dictionary"""

        logger.debug("im: flushing %s" % str(partition))

        partitions = {}

        for p, tuples in partition:

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
        """returns an iterator for a reduce task input"""

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
        """return next partition's files list"""
        try:
            with self._lock:
                p, fs = self._partitions.popitem()
        except KeyError:
            raise StopIteration

        return fs


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

        self.maptask = MapWrapper(self.map('MapTask'), self.im, self)

        self.reducetask = ReduceWrapper(self.reduce('ReduceTask'), self.im, self)


    def map_callback(self, result, mapid=None, local=False):
        """called on a map task completion"""

        logger.debug('   map_callback %s: %s' % (mapid, result))
        self.im.update_partitions(result)

        try:
            del self.map_tasks[mapid]
        except KeyError:
            logger.debug('   map_callback: no such task -> %s' % mapid)

        # more work?
        self.map_next(local)


    def reduce_callback(self, result, reduceid=None, local=False):
        """called on a reduce task completion"""

        logger.debug('   reduce_callback %s: %s' % (reduceid, result))
        self.output.update(result)

        try:
            del self.reduce_tasks[reduceid]
        except KeyError:
            logger.debug('   reduce_callback: no such task -> %s' % reduceid)

        # more work?
        self.reduce_next(local)


    def _start(self, args, callback, callback_args={}):
        """overridden to prevent early cleanup.
        
        MapReduceTask does not implement work() and don't expect user to provide its own. Instead it requires user to provide map and reduce attributes which should be classes derived from MapTask and ReduceTask respectively.

        Cleanup is in _complete() which will be called when there is no more work remaining.
        map:
        * work(): initialization and calling map_next() for every worker available;
        * map_next(): checking if any data to process and starting a map task,
          if no more data available, call reduce_stage();
        * map_callback(): updating results (partition) and calling map_next() for more work.
        
        reduce:
        * reduce_stage: calling reduce_next() for every worker available;
        * reduce_next(): checking if any data to process and starting a reduce task,
          if no more data available, call _complete();
        * reduce_callback(): updating results (output) and calling reduce_next() for more work.

        _complete:
        * cleanup and callbacks.
        """

        self.__callback = callback
        self._callback_args = callback_args

        self._status = STATUS_RUNNING

        self._reduce_called = False
        self._complete_called = False

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
        """more work for a map task"""

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
            self.maptask._start(args=map_args, callback=self.map_callback,
                                            callback_args={'mapid': mapid})
        else:
            if local: # XXX orginal worker is to run computations as well, or schedule only?
                logger.debug("mapreduce: running locally %s" % mapid)
                self.maptask.start(args=map_args, callback=self.map_callback,
                                    callback_args={'mapid': mapid, 'local': local})
            else:
                logger.debug("mapreduce: requesting worker for %s: %s"
                        % (mapid, self.maptask.get_key()) )
                self.parent.request_worker(self.maptask.get_key(), map_args, mapid)


    def reduce_stage(self):
        """starting a reduce stage"""

        if self.map_tasks:
            logger.debug('mapreduce: waiting for map stage to finish')
            reactor.callLater(1, self.reduce_stage)
            return


        self._partition_iter = enumerate(self.im)

        logger.debug('mapreduce: reduce stage')

        if self._available_workers > 1 and self.sequential is False:
            for i in range(1, self._available_workers):
                self.reduce_next(local=False)

        self.reduce_next(local=True)


    def reduce_next(self, local=False):
        """more work for reduce task"""

        try:
            id, p = self._partition_iter.next()
        except StopIteration:
            # call task complete (final stage)
            if not self._complete_called:
                self._complete_called = True
                self._complete()

            return


        reduceid = 'reduce%d' % id
        self.reduce_tasks[reduceid] = 1

        logger.debug('   starting reducetask: %s' % reduceid)
        reduce_args = {
                        'partition': p,
                      }

        if self.sequential:
            self.reducetask._start(args=reduce_args, callback=self.reduce_callback,
                                callback_args={'reduceid': reduceid})
        else:
            if local: # XXX orginal worker is to run computations as well, or schedule only?
                logger.debug("mapreduce: running locally %s" % reduceid)
                self.reducetask.start(args=reduce_args, callback=self.reduce_callback,
                                        callback_args={'reduceid': reduceid, 'local': local})
            else:
                logger.debug("mapreduce: requesting worker for %s: %s"
                        % (reduceid, self.reducetask.get_key()) )
                self.parent.request_worker(self.reducetask.get_key(), reduce_args, reduceid)


    def _work_unit_complete(self, result, id):
        """retrieving results form remote task"""

        logger.debug("mapreduce: got REMOTE result %s from %s" % (result, id))

        #check if map or reduce task
        if id in self.map_tasks:
            self.map_callback(result, id, local=False)

        elif id in self.reduce_tasks:
            self.reduce_callback(result, id, local=False)


    def _complete(self):
        """
        Should be called when all map and reduce task have completed
        """

        if self.reduce_tasks:
            logger.debug('mapreduce: waiting for reduce stage to finish')
            reactor.callLater(1, self._complete)
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


class MapReduceWrapper():
    """map-reduce wrapper base class.

    It expects to work() to do some speciall stuff before and after subtask (self.task) real wrok() method.

    It stores intermediate results helper (self.im) and overrides:
    * _generate_key() to assure proper subtask identification,
    * get_subtask() to return self instead of subtask directly,
    * start() to run special self.work() instead of subtask's"""

    def __init__(self, task, im, parent):
        self.task = task
        self.im = im
        self.parent = parent
        self.task.parent = parent


    def _generate_key(self):
        return self.task._generate_key()


    def get_key(self):
        return self._generate_key()


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


    def start(self, args={}, subtask_key=None, callback=None, callback_args={}, errback=None):
        """
        starts the task.  This will spawn the work in a workunit thread.
        """

        # only start if not already running
        if self.task._status == STATUS_RUNNING:
            return

        logger.debug('MapReduceWrapper - starting task: %s' % args)
        self.work_deferred = threads.deferToThread(self._start, args, callback, callback_args)

        if errback:
            self.work_deferred.addErrback(errback)

        return 1


class MapWrapper(MapReduceWrapper):

    def _start(self, args={}, callback=None, callback_args={}):
        """
        Overwrites Task._start() because MapTask needs to provide special input and output
        dictionaries. And it is necessary to self.im.flush() intermediate results after
        self._start().
        """
        logger.debug('%s - MapWrapper.work()'  % self.get_worker().worker_key)

        output = AppendableDict()
        args['output'] = output

        id = args['id']
        logger.debug("%s._work()" % id)

        self.task._work(**args) # ignoring results

        pdict = self.im.partition_output(output)
        results = self.im.flush(pdict, id) # partitions are our results

        logger.debug('%s - MapWrapper - work complete' % self.get_worker().worker_key)

        #make a callback, if any
        if callback:
            logger.debug('%s - MapWrapper - Making callback' % self)
            callback(results, **callback_args)
        else:
            logger.warning('%s - MapWrapper - NO CALLBACK TO MAKE: %s' % (self, callback))

        return results


class ReduceWrapper(MapReduceWrapper):

    def _start(self, args={}, callback=None, callback_args={}):
        """
        Overwrites Task._start() beacuse ReduceTask needs to provide special input
        dictionaries (from self.im).
        """
        logger.debug('%s - ReduceWrapper.work()'  % self.get_worker().worker_key)

        args['input'] = self.im._partition_iter(args['partition'])
        output = args['output'] = {}

        #id = args['id']
        #logger.debug("%s._work()" % id)

        self.task._work(**args) # ignoring results
        results = output

        logger.debug('%s - ReduceWrapper - work complete' % self.get_worker().worker_key)

        #make a callback, if any
        if callback:
            logger.debug('%s - ReduceWrapper - Making callback' % self)
            callback(results, **callback_args)
        else:
            logger.warning('%s - ReduceWrapper - NO CALLBACK TO MAKE: %s' % (self, callback))

        return results

