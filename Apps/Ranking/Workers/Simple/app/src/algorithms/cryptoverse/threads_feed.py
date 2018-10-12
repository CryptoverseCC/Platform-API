"""
Cryptoverse feed with replies and likes
=======================================

Version: 0.1.0

Example:

`ranking/cryptoverse_feed <https://api.userfeeds.io/ranking/cryptoverse_feed>`_

"""

from algorithms.cryptoverse import threads_root as root
from algorithms.kuba import replies, reactions


def run(conn_mgr, input, **params):
    result = root.run(conn_mgr, input, **params)
    result = replies.run(conn_mgr, result)
    result = reactions.run(conn_mgr, result)
    set_types(result["items"])
    return result


def set_types(items):
    for i in items:
        if i["about"]:
            i["type"] = "post_about"
        else:
            i["type"] = "regular"
