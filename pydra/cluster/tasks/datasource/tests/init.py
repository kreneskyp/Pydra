#!/usr/bin/env python

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

class UnpackTest(unittest.TestCase):

    pass

if __name__ == "__main__":
    unittest.main()
