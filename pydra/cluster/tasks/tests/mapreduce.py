import unittest

import tempfile, shutil

from pydra.cluster.tasks.mapreduce import *
from pydra.cluster.tasks.tasks import Task
from pydra.task_cache.mapreduce import *
from proxies import *


class AppendableDict_Test(unittest.TestCase):

    def test_append(self):
        a = AppendableDict()

        self.assertRaises(KeyError, lambda: a['key'])

        a['key'] = 1
        self.assert_(1 in a['key'])
        self.assert_(2 not in a['key'])

        a['key'] = 2
        self.assert_(1 in a['key'])
        self.assert_(2 in a['key'])
        self.assert_(3 not in a['key'])


class IntermediateResultsFiles_Test(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.task_name = "test_task"
        self.dir = DatasourceDir(self.tempdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_partition(self):

        a = { 'a': 1, 'b': 1, }

        im1 = IntermediateResultsFiles(self.dir) 
        im1.task_id = self.task_name
        im1.reducers = 2

        pdict = im1.partition_output(a)
        p1 = im1.dump(pdict, 'map1')

        b = { 'b': 1, 'c': 1, }

        im2 = IntermediateResultsFiles(self.dir) 
        im2.task_id = self.task_name
        im2.reducers = 2

        pdict = im2.partition_output(b)
        p2 = im2.dump(pdict, 'map2')

        # getting results
        im = IntermediateResultsFiles(self.dir) 
        im.task_id = self.task_name
        im.reducers = 2

        im.update_partitions(p1)
        im.update_partitions(p2)

        # reduce
        c = { 'a': 0, 'b': 0, 'c': 0 }
        for p in im:
            for k, v in im.load(p):
                c[k] += 1

        self.assertEqual(c['a'], 1)
        self.assertEqual(c['b'], 2)
        self.assertEqual(c['c'], 1)


class MapReduceTask_Test(unittest.TestCase):
    """
    Tests for verify functionality of MapReduceTask class
    """

    def setUp(self):
        self.mapreduce_task = CountWords()
        self.worker = WorkerProxy()
        self.mapreduce_task.parent = self.worker


    def tearDown(self):
        pass


    def test_key_generation_mapreducetask(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'CountWords'
        key = self.mapreduce_task.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )


    def test_key_generation_mapreducetask_child(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """

        # test for MapTask
        expected = 'CountWords.MapWords'
        key = self.mapreduce_task.maptask.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )

        # test for ReduceTask
        expected = 'CountWords.ReduceWords'
        key = self.mapreduce_task.reducetask.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )


    def test_get_subtask_mapreducetask(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key
        key = 'CountWords'
        expected = self.mapreduce_task
        returned = self.mapreduce_task.get_subtask(key.split('.'))
        self.assert_(returned is expected, 'Subtask retrieved was not the expected Task')

        # incorrect Key
        key = 'FakeTaskThatDoesNotExist'
        self.assertRaises(TaskNotFoundException, self.mapreduce_task.get_subtask, key.split('.'))


    def test_get_subtask_mapreducetask_child(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key for maptask
        key = 'CountWords.MapWords'
        expected = self.mapreduce_task.maptask
        returned = self.mapreduce_task.get_subtask(key.split('.'))
        self.assert_(returned is expected, 'MapTask retrieved was not the expected Task')

        # correct key for reducetask
        key = 'CountWords.ReduceWords'
        expected = self.mapreduce_task.reducetask
        returned = self.mapreduce_task.get_subtask(key.split('.'))
        self.assert_(returned is expected, 'ReduceTask retrieved was not the expected Task')

        # incorrect Key
        key = 'CountWords.FakeTaskThatDoesNotExist'
        self.assertRaises(TaskNotFoundException, self.mapreduce_task.get_subtask, key.split('.'))


    def test_get_worker_mapreducetask(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.mapreduce_task.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assert_(returned is self.worker, 'worker retrieved was not the expected worker')


    def test_get_worker_mapreducetask_child(self):
        """
        Verifies that the worker can be retrieved for maptask and reducetask
        """
        returned = self.mapreduce_task.maptask.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')

        returned = self.mapreduce_task.reducetask.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')


class IdentityMapTask(Task):

    def _work(self, input, output, **kwargs):

        for k, v in input:
            output[k] = v


class IdentityReduceTask(Task):

    def _work(self, input, output, **kwargs):

        for k, v in input:
            output[k] = v


class NullIM():
    """dummy intermediate results class"""

    def partition_output(self, output):
        return output


    def dump(self, output, mapid):
        return output, mapid


    def load(self, fs):
        return fs.iteritems()


class MapReduceWrapper_Test(unittest.TestCase):

    def setUp(self):
        self.im = NullIM()

        self.worker = WorkerProxy()

        self.maptask = MapWrapper(IdentityMapTask("IdentityMapTask"), self.im, self.worker)
        #self.maptask.parent = self.worker

        self.reducetask = ReduceWrapper(IdentityReduceTask("IdentityReduceTask"), self.im, self.worker)
        #self.reducetask.parent = self.worker


    def test_work_mapwrapper(self):
        a = { 'a': 1, 'b': 1, }
        id = 'identity_map'

        dump_results = self.maptask._start(args={'input': a.iteritems(), 'id': id})
        output, mapid = dump_results

        self.assertEqual(mapid, id, "mapid differs from id")

        for k, v in a.iteritems():
            self.assert_(v in output[k])


    def test_work_reducewrapper(self):
        a = { 'a': 1, 'b': 1, }

        results = self.reducetask._start(args={'partition': a})

        for k, v in a.iteritems():
            self.assert_(v == results[k])


    def test_get_subtask(self):
        # checking if the wrappper returns self instead of a task its wrapping

        key = "IdentityMapTask"
        expected = self.maptask
        returned = self.maptask.get_subtask(key.split('.'))
        self.assert_(returned is expected, 'MapTask retrieved was not the expected Task')

        key = "IdentityReduceTask"
        expected = self.reducetask
        returned = self.reducetask.get_subtask(key.split('.'))
        self.assert_(returned is expected, 'ReduceTask retrieved was not the expected Task')

