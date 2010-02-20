#!/usr/bin/env python

import unittest

from pydra.cluster.tasks.datasource.slicer import IterSlicer, MapSlicer, LineSlicer

class IterSlicerTest(unittest.TestCase):

    def setUp(self):

        self.l = [1, 2, 3]
        self.slicer = IterSlicer(self.l)

    def test_trivial(self):

        self.assertEqual(self.l, [i for i in self.slicer])

    def test_key(self):

        self.assertTrue(hasattr(self.slicer, "key") and self.slicer.key)

class MapSlicerTest(unittest.TestCase):

    def setUp(self):

        self.d = {1 : 2, 3 : 4}
        self.slicer = MapSlicer(self.d)

    def test_trivial(self):

        self.assertEqual(self.d.keys(), [k for k in self.slicer])

    def test_key(self):

        self.assertTrue(hasattr(self.slicer, "key") and self.slicer.key)

class LineSlicerTest(unittest.TestCase):

    def setUp(self):

        self.s = """
            Jackdaws love my big sphinx of quartz.
            The quick brown fox jumps over the lazy dog.
            """
        self.slicer = LineSlicer(self.s)

    def test_trivial(self):

        self.assertEqual([51, 108], [pos for pos in self.slicer])

    def test_key(self):

        self.assertTrue(hasattr(self.slicer, "key") and self.slicer.key)

if __name__ == "__main__":
    unittest.main()
