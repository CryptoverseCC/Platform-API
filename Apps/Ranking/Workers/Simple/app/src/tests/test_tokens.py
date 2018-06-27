import unittest

from algorithms.experimental.tokens import filter_owned_tokens


class TestTokens(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(len(filter_owned_tokens([], [])), 0)

    def test_return_received_token(self):
        self.assertEqual(len(filter_owned_tokens([{
            "asset": "0x0",
            "token": "1",
            "sequence": 1
        }], [

        ])), 1)

    def test_not_return_send_token(self):
        self.assertEqual(len(filter_owned_tokens([{
            "asset": "0x0",
            "token": "1",
            "sequence": 1
        }], [{
            "asset": "0x0",
            "token": "1",
            "sequence": 2
        }])), 0)

    def test_not_crash_when_token_only_send(self):
        self.assertEqual(len(filter_owned_tokens([

        ], [{
            "asset": "0x0",
            "token": "1",
            "sequence": 2
        }])), 0)

    def test_return_token_when_received_again(self):
        self.assertEqual(len(filter_owned_tokens([{
            "asset": "0x0",
            "token": "1",
            "sequence": 1
        }, {
            "asset": "0x0",
            "token": "1",
            "sequence": 3
        }], [{
            "asset": "0x0",
            "token": "1",
            "sequence": 2
        }])), 1)

    def test_return_not_send_tokens(self):
        self.assertEqual(filter_owned_tokens([{
            "asset": "0x0",
            "token": "1",
            "sequence": 1
        }, {
            "asset": "0x0",
            "token": "2",
            "sequence": 1
        }], [{
            "asset": "0x0",
            "token": "1",
            "sequence": 2
        }]), [{
            "asset": "0x0",
            "token": "2",
            "sequence": 1
        }])

    def test_group_by_token(self):
        self.assertEqual(filter_owned_tokens([{
            "asset": "0x0",
            "token": "1",
            "sequence": 1
        }, {
            "asset": "0x1",
            "token": "1",
            "sequence": 1
        }], [

        ]), [{
            "asset": "0x0",
            "token": "1",
            "sequence": 1
        }, {
            "asset": "0x1",
            "token": "1",
            "sequence": 1
        }])

    def test_complex(self):
        self.assertEqual(filter_owned_tokens([{
            "asset": "0x0",
            "token": "135",
            "sequence": 5854986
        }, {
            "asset": "0x1",
            "token": "6180",
            "sequence": 5410744
        }, {
            "asset": "0x0",
            "token": "593163",
            "sequence": 5296837
        }], [
            # no sent tokens
        ]), [{
            "asset": "0x0",
            "token": "135",
            "sequence": 5854986
        }, {
            "asset": "0x0",
            "token": "593163",
            "sequence": 5296837
        }, {
            "asset": "0x1",
            "token": "6180",
            "sequence": 5410744
        }])


if __name__ == '__main__':
    unittest.main()
