import unittest

import tempfile, shutil

from pydra_server.cluster.tasks.mapreduce import *


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

        c = { 'a': 0, 'b': 0, 'c': 0 }
        for p in im:
            for k, v in p:
                c[k] += 1

        self.assertEqual(c['a'], 1)
        self.assertEqual(c['b'], 2)
        self.assertEqual(c['c'], 1)

