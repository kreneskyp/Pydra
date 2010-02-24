#!/usr/bin/env python

import unittest

from pydra.cluster.tasks.datasource.key import save_class, restore_class

class InstanceTest(unittest.TestCase):

    def setUp(self):
        import socket
        self.sock_class = socket.socket
        del socket

    def test_save_restore(self):
        saved_class = save_class(self.sock_class)
        restored_class = restore_class(saved_class)
        self.assertEqual(restored_class, self.sock_class)

if __name__ == "__main__":
    import os.path
    os.chdir(os.path.dirname(__file__))
    unittest.main()
