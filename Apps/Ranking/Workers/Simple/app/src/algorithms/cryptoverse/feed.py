"""
Cryptoverse feed with replies and likes
=======================================

Version: 0.1.0

Example:

`ranking/cryptoverse_feed <https://api.userfeeds.io/ranking/cryptoverse_feed>`_

"""

from algorithms.cryptoverse import root
from algorithms.kuba import replies, reactions


def run(conn_mgr, input, **params):
    result = root.run(conn_mgr, input, **params)
    result = replies.run(conn_mgr, result)
    result = reactions.run(conn_mgr, result)
    return result
