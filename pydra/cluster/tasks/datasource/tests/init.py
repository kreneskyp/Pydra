#!/usr/bin/env python

import itertools
import unittest

from pydra.cluster.tasks.datasource import unpack, validate
from pydra.cluster.tasks.datasource.slicer import IterSlicer

class ValidateTest(unittest.TestCase):

    def test_none(self):
        ds = validate(None)
        self.assertEqual(ds, (IterSlicer, [None]))

    def test_string(self):
        s = "Make it so, Number One!"
        ds = validate(s)
        self.assertEqual(ds, (IterSlicer, s))

    def test_iterslicer(self):
        ds = (IterSlicer, [1, 2, 3, 4, 5])
        self.assertEqual(ds, validate(ds))

    def test_args(self):
        ds = validate(IterSlicer, [1, 2, 3, 4, 5])
        self.assertEqual(ds, validate(ds))

class UnpackTest(unittest.TestCase):

    def test_iterslicer(self):
        l = [chr(i) for i in range(255)]
        u = u"\u03c0 \u042f \u97f3 \u00e6 \u221e"
        s = "Aye aye, Cap'n."
        t = (True, False, None)
        x = xrange(10)

        for i in l, u, s, t, x:
            ds = validate((IterSlicer, i))
            for expected, unpacked in itertools.izip_longest(i,
                unpack(ds)):
                self.assertEqual(expected, unpacked)

    def test_iterslicer_unvalidated(self):
        l = [chr(i) for i in range(255)]
        u = u"\u03c0 \u042f \u97f3 \u00e6 \u221e"
        s = "Aye aye, Cap'n."
        t = (True, False, None)
        x = xrange(10)

        for i in l, u, s, t, x:
            ds = IterSlicer, i
            for expected, unpacked in itertools.izip_longest(i,
                unpack(ds)):
                self.assertEqual(expected, unpacked)

if __name__ == "__main__":
    unittest.main()
