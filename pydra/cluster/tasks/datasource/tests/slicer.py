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
            """
        slicer = LineSlicer(s)
        self.assertEqual(s.split("\n"), [line for line in slicer])

if __name__ == "__main__":
    unittest.main()
