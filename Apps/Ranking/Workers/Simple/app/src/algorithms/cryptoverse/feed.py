"""
Cryptoverse feed with replies and likes
=======================================

Version: 0.1.0

Example:

`ranking/cryptoverse_feed <https://api.userfeeds.io/ranking/cryptoverse_feed>`_

"""

import re
from algorithms.cryptoverse import root
from algorithms.kuba import replies, reactions


tokenPattern = re.compile("[a-z]+:0x[0-9a-f]{40}:\d+")


def run(conn_mgr, input, **params):
    result = root.run(conn_mgr, input, **params)
    result = replies.run(conn_mgr, result)
    result = reactions.run(conn_mgr, result)
    set_types(result["items"])
    return result


def set_types(items):
    for i in items:
        if tokenPattern.match(i["target"]):
            i["type"] = "follow"
        elif i["about"]:
            if tokenPattern.match(i["about"]):
                i["type"] = "post_to"
            else:
                i["type"] = "post_about"
        else:
            i["type"] = "regular"
