import unittest

import tempfile, shutil

from pydra_server.cluster.tasks.mapreduce import *
from pydra_server.task_cache.mapreduce import *
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

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_partition(self):

        a = { 'a': 1, 'b': 1, }

        im1 = IntermediateResultsFiles(self.task_name, 2, self.tempdir) 
        p1 = im1.flush(a, 'map1')

        b = { 'b': 1, 'c': 1, }

        im2 = IntermediateResultsFiles(self.task_name, 2, self.tempdir) 
        p2 = im2.flush(b, 'map2')

        # getting results
        im = IntermediateResultsFiles(self.task_name, 2, self.tempdir) 
        im.update_partitions(p1)
        im.update_partitions(p2)

        # reduce
        c = { 'a': 0, 'b': 0, 'c': 0 }
        for p in im:
            for k, v in im._partition_iter(p):
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
        self.assertEqual(returned, expected, 'Subtask retrieved was not the expected Task')

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
        self.assertEqual(returned, expected, 'MapTask retrieved was not the expected Task')

        # correct key for reducetask
        key = 'CountWords.ReduceWords'
        expected = self.mapreduce_task.reducetask
        returned = self.mapreduce_task.get_subtask(key.split('.'))
        self.assertEqual(returned, expected, 'ReduceTask retrieved was not the expected Task')

        # incorrect Key
        key = 'CountWords.FakeTaskThatDoesNotExist'
        self.assertRaises(TaskNotFoundException, self.mapreduce_task.get_subtask, key.split('.'))


    def test_get_worker_mapreducetask(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.mapreduce_task.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')


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


class IdentityMapTask(MapTask):

    def _work(self, input, output, **kwargs):

        for k, v in input:
            output[k] = v


class IdentityReduceTask(ReduceTask):

    def _work(self, input, output, **kwargs):

        for k, v in input:
            output[k] = v


class NullIM():
    """dummy intermediate results class"""

    def flush(self, output, mapid):
        return output, mapid


    def _partition_iter(self, fs):
        return fs.iteritems()


class MapReduceSubtask_Test(unittest.TestCase):

    def setUp(self):
        self.im = NullIM()

        self.worker = WorkerProxy()

        self.maptask = IdentityMapTask("IdentityMapTask", self.im)
        self.maptask.parent = self.worker

        self.reducetask = IdentityReduceTask("IdentityReduceTask", self.im)
        self.reducetask.parent = self.worker


    def test_work_maptask(self):
        a = { 'a': 1, 'b': 1, }
        id = 'identity_map'

        flush_results = self.maptask.work(args={'input': a.iteritems(), 'id': id})
        output, mapid = flush_results

        self.assertEqual(mapid, id, "mapid differs from id")

        for k, v in a.iteritems():
            self.assert_(v in output[k])


    def test_work_reducetask(self):
        a = { 'a': 1, 'b': 1, }

        results = self.reducetask.work(args={'partition': a})

        for k, v in a.iteritems():
            self.assert_(v == results[k])

