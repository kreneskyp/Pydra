#!/usr/bin/env python

import unittest

from pydra.cluster.tasks.datasource.backend import SQLBackend

class InstanceTest(unittest.TestCase):

    def test_sqlite(self):
        sb = SQLBackend("sqlite")
        sb.connect(":memory:")

if __name__ == "__main__":
    unittest.main()
