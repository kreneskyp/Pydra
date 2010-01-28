#!/usr/bin/env python

import unittest

from pydra.cluster.tasks.datasource.selector import DirSelector

class DirSelectorTest(unittest.TestCase):

    def test_cheeses(self):

        ds = DirSelector("cheeses")
        self.assertEqual(len(ds), 1)

class FileSelectorTest(unittest.TestCase):
    pass

if __name__ == "__main__":
    unittest.main()
