import logging
import unittest

import pydra.util

logging.basicConfig(level=logging.DEBUG)

class InitTest(unittest.TestCase):

    def test_deprecated(self):
        @pydra.util.deprecated("Testing deprecation markings")
        def f():
            pass

        f()

if __name__ == "__main__":
    unittest.main()
