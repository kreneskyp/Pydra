#!/usr/bin/env python

import unittest

from pydra.cluster.tasks.datasource.backend import SQLBackend

class SQLTest(unittest.TestCase):

    def test_sqlite(self):
        sb = SQLBackend("sqlite", ":memory:")
        self.assertTrue(sb.connected)

    def test_sqlite_reuse(self):
        sb = SQLBackend("sqlite", ":memory:")
        self.assertTrue(sb.connected)
        sb.disconnect()
        self.assertFalse(sb.connected)
        sb.connect(":memory:")

if __name__ == "__main__":
    unittest.main()
