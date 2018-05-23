import unittest

from algorithms.experimental.balance_of import *


class BalanceOfTest(unittest.TestCase):

    def test_balance_is_equal_to_single_received_transfer(self):
        received_transfers = [create_transfer('0x1', 'kot', '4')]
        send_transfers = []
        results = balances_of(received_transfers, send_transfers)
        self.assertEqual(results, [
            create_identity_balance('0x1', 'kot', 4)
        ])

    def test_sum_received_and_send_transfers(self):
        received_transfers = [create_transfer('0x1', 'kot', '4')]
        send_transfers = [create_transfer('0x1', 'kot', '2')]
        results = balances_of(received_transfers, send_transfers)
        self.assertEqual(results, [
            create_identity_balance('0x1', 'kot', 2)
        ])

    def test_not_sum_transfers_of_different_assets(self):
        received_transfers = [create_transfer('0x1', 'kot', '4')]
        send_transfers = [create_transfer('0x1', 'pies', '2')]
        results = balances_of(received_transfers, send_transfers)
        self.assertEqual(results, [
            create_identity_balance('0x1', 'pies', -2),
            create_identity_balance('0x1', 'kot', 4)
        ])

    def test_not_sum_transfers_owned_by_different_identities(self):
        received_transfers = [create_transfer('0x1', 'kot', '4'),
                              create_transfer('0x2', 'kot', '4')]
        send_transfers = []
        results = balances_of(received_transfers, send_transfers)
        self.assertEqual(results, [
            create_identity_balance('0x1', 'kot', 4),
            create_identity_balance('0x2', 'kot', 4)
        ])

    def test_sum_received_transfers_owned_by_same_identity(self):
        received_transfers = [create_transfer('0x1', 'kot', '4'),
                              create_transfer('0x1', 'kot', '11')]
        send_transfers = []
        results = balances_of(received_transfers, send_transfers)
        self.assertEqual(results, [
            create_identity_balance('0x1', 'kot', 15)
        ])

    def test_substract_sum_of_send_transfers_from_sum_of_received_transfers(self):
        received_transfers = [create_transfer('0x1', 'kot', '4'), create_transfer('0x1', 'kot', '4')]
        send_transfers = [create_transfer('0x1', 'kot', '2'), create_transfer('0x1', 'kot', '2')]
        results = balances_of(received_transfers, send_transfers)
        self.assertEqual(results, [
            create_identity_balance('0x1', 'kot', 4)
        ])


def create_identity_balance(identity, asset, balance):
    return {
        'identity': identity,
        'asset': asset,
        'balance': balance
    }


def create_transfer(identitiy, asset, amount):
    return {'identity': identitiy, 'asset': asset, 'amount': amount}
