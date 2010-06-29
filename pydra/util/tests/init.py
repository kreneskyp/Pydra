import unittest

import pydra.util

class InitTest(unittest.TestCase):

    def test_deprecated(self):
        @pydra.util.deprecated
        def f():
            pass

        f()

if __name__ == "__main__":
    unittest.main()
