import os
import sys
import unittest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

if __name__ == '__main__':
    loader = unittest.TestLoader()
    tests = loader.discover(os.path.dirname(__file__))
    testRunner = unittest.runner.TextTestRunner()
    exitCode = not testRunner.run(tests).wasSuccessful()
    sys.exit(exitCode)
