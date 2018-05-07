import unittest
from algorithms.experimental.claims import normalize_to_list


class NormalizeToListTest(unittest.TestCase):

    def test_given_list_returns_it(self):
        result = normalize_to_list(["123"])
        self.assertTrue(isinstance(result, list))

    def test_given_not_list_wraps_it(self):
        result = normalize_to_list("123")
        self.assertTrue(isinstance(result, list))
