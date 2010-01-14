#!/usr/bin/env python

import unittest

from pydra.cluster.tasks.datasource.slicer import IterSlicer, MapSlicer, LineSlicer

class IterSlicerTest(unittest.TestCase):

    def test_trivial(self):

        l = [1, 2, 3]
        slicer = IterSlicer(l)
        self.assertEqual(l, [i for i in slicer])

class MapSlicerTest(unittest.TestCase):

    def test_trivial(self):

        d = {1 : 2, 3 : 4}
        slicer = MapSlicer(d)
        self.assertEqual(d.keys(), [k for k in slicer])

class LineSlicerTest(unittest.TestCase):

    def test_trivial(self):

        s = """
            Jackdaws love my big sphinx of quartz.
            The quick brown fox jumps over the lazy dog.
            """
        slicer = LineSlicer(s)
        self.assertEqual([51, 108], [pos for pos in slicer])

if __name__ == "__main__":
    unittest.main()
