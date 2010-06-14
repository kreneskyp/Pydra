#!/usr/bin/env python

import unittest

from pydra.util.key import save_class, restore_class

class InstanceTest(unittest.TestCase):

    def test_trivial(self):
        import socket
        sock_class = socket.socket
        del socket

        saved_class = save_class(sock_class)
        restored_class = restore_class(saved_class)
        self.assertEqual(restored_class, sock_class)

    def test_deep(self):
        from xml.sax.handler import ContentHandler
        ch_class = ContentHandler
        del ContentHandler

        saved_class = save_class(ch_class)
        restored_class = restore_class(saved_class)
        self.assertEqual(restored_class, ch_class)

if __name__ == "__main__":
    import os.path
    os.chdir(os.path.dirname(__file__))
    unittest.main()
