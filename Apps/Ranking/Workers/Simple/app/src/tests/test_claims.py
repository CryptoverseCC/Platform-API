import unittest
from algorithms.experimental.claims import *


class ClaimTest(unittest.TestCase):

    def test_given_id_look_for_claim_with_id(self):
        params = {
            "id": ["123"]
        }
        expected_query = FIND_CLAIMS + "WHERE " + FILTER_BY_ID + RETURN
        query = build_query(params)
        self.assertEqual(query, expected_query)

    def test_given_target_look_for_claim_with_target(self):
        params = {
            "target": ["123"]
        }
        expected_query = FIND_CLAIMS + "WHERE " + FILTER_BY_TARGETS + RETURN
        query = build_query(params)
        self.assertEqual(query, expected_query)

    def test_given_author_look_for_claim_with_author(self):
        params = {
            "author": ["123"]
        }
        expected_query = FIND_CLAIMS + "WHERE " + FILTER_BY_AUTHORS + RETURN
        query = build_query(params)
        self.assertEqual(query, expected_query)

    def test_given_target_with_author_look_for_both(self):
        params = {
            "author": ["123"],
            "target": ["123"]
        }
        expected_query = FIND_CLAIMS + "WHERE " + FILTER_BY_TARGETS + "AND " + FILTER_BY_AUTHORS + RETURN
        query = build_query(params)
        self.assertEqual(query, expected_query)
