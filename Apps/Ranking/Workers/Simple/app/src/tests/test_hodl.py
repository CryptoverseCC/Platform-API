import unittest
from algorithms.experimental.hodl import calculate_hodl


class TestPosts(unittest.TestCase):

    def test(self):
        transfers = [
            {
                "asset": "ethereum",
                "sequence": 100,
                "amount": 1
            }
        ]
        max_packages = {
            "ethereum": 200
        }
        self.assertListEqual([{}], [{}])
        self.assertEqual(calculate_hodl(max_packages, transfers), [
            {
                "asset": "ethereum",
                "value": 100
            }
        ])
