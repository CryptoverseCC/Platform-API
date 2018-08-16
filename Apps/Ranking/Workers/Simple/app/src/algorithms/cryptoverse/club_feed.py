"""
Cryptoverse club feed with replies and likes
============================================

Version: 0.1.0

Example:

`ranking/cryptoverse_feed <https://api.userfeeds.io/ranking/cryptoverse_feed>`_

"""

from algorithms.utils import tokenPattern, addressPattern
from algorithms.cryptoverse import club
from algorithms.kuba import replies, reactions
from algorithms.utils import param


@param("id", required=True)
def run(conn_mgr, input, **params):
    result = club.run(conn_mgr, input, **params)
    result = replies.run(conn_mgr, result)
    result = reactions.run(conn_mgr, result)
    set_types(result["items"])
    return result


def set_types(items):
    for i in items:
        if tokenPattern.match(i["target"]) or addressPattern.match(i["target"]):
            i["type"] = "boost"
        elif i.get("about"):
            if tokenPattern.match(i["about"]):
                i["type"] = "post_to"
            elif addressPattern.match(i["about"]):
                i["type"] = "post_to_simple"
            else:
                i["type"] = "post_about"
        else:
            i["type"] = "regular"
