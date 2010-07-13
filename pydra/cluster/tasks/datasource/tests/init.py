#!/usr/bin/env python

import unittest

from pydra.cluster.tasks.datasource import unpack, validate
from pydra.cluster.tasks.datasource.slicer import IterSlicer

class ValidateTest(unittest.TestCase):

    def test_none(self):
        ds = validate(None)
        self.assertEqual(ds, (IterSlicer, [None]))

class UnpackTest(unittest.TestCase):

    pass

if __name__ == "__main__":
    unittest.main()
