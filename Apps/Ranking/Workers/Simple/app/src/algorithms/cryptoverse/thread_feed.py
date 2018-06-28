"""
Cryptoverse thread feed with likes
==================================

Version: 0.1.0

Example:

`ranking/cryptoverse_thread_feed;id=claim:0x5bad1020a...6a9b13301b <https://api.userfeeds.io/ranking/cryptoverse_thread_feed;id=claim:0x5bad1020a14a58f358363c35eb4fa3d3eb3b1c58c160196e810e77771205444e7b7336e9c42b99960f189e8e828c9643f6fcf4e42233a0473a5570ee6a9b13301b>`_

"""

import re
from algorithms.cryptoverse import thread
from algorithms.kuba import reactions

tokenPattern = re.compile("[a-z]+:0x[0-9a-f]{40}:\d+")
claimPattern = re.compile("claim:0x[0-9a-f]+(:\d+)?")


@param("id", required=True)
def run(conn_mgr, input, **params):
    result = thread.run(conn_mgr, input, **params)
    result = reactions.run(conn_mgr, result)
    set_types(result["items"])
    return result


def set_types(items):
    for i in items:
        if not isinstance(i["target"], str):
            i["type"] = "like"
            set_type(i["target"])
        else:
            set_type(i)

def set_type(i):
    if tokenPattern.match(i["target"]):
        i["type"] = "follow"
    elif i["about"]:
        if tokenPattern.match(i["about"]):
            i["type"] = "post_to"
        elif claimPattern.match(i["about"]):
            i["type"] = "response"
        else:
            i["type"] = "post_about"
    else:
        i["type"] = "regular"
