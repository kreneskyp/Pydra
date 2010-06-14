#!/usr/bin/env python

import unittest

from pydra.cluster.tasks.datasource.slicer import IterSlicer, MapSlicer, LineSlicer
from pydra.util.key import thaw

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
            Pack my box with five dozen liquor jugs.
            """
        self.slicer = LineSlicer(self.s)

    def test_trivial(self):

        self.assertEqual([51, 108, 161], [pos for pos in self.slicer])

    def test_key(self):

        self.assertTrue(hasattr(self.slicer, "key") and self.slicer.key)

    def test_state(self):

        l = [next(self.slicer)]
        saved = self.slicer.key
        restored = thaw(saved)
        l.append(next(restored))
        self.assertEqual([51, 108], l)

    def test_state_slice(self):

        self.slicer.state = slice(50, 150)
        self.assertEqual([51, 108], [pos for pos in self.slicer])
        self.slicer.state = slice(100, 200)
        self.assertEqual([108, 161], [pos for pos in self.slicer])

    def test_getitem(self):

        ls = self.slicer[50:100]
        self.assertEqual([51, 108], [pos for pos in ls])

if __name__ == "__main__":
    unittest.main()
