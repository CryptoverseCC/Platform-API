"""
Cryptoverse single feed with replies and likes
==============================================

Version: 0.1.0

Example:

`ranking/cryptoverse_single_feed;id=ethereum:0x06012...a266d:341605 <https://api.userfeeds.io/ranking/cryptoverse_single_feed;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:341605>`_

"""

import re
from algorithms.cryptoverse import single
from algorithms.cryptoverse import single_address
from algorithms.kuba import replies, reactions
from algorithms.utils import param

tokenPattern = re.compile("[a-z]+:0x[0-9a-f]{40}:\d+")
assetPattern = re.compile("[a-z]+:0x[0-9a-f]{40}")
addressPattern = re.compile("0x[0-9a-f]{40}")


@param("id", required=True)
def run(conn_mgr, input, **params):
    if addressPattern.match(params["id"]):
        result = single_address.run(conn_mgr, input, **params)
    else:
        result = single.run(conn_mgr, input, **params)
    result = replies.run(conn_mgr, result)
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
    elif addressPattern.match(i["target"]):
        i["type"] = "follow_simple"
    elif i.get("label") in ["github", "twitter", "instagram", "facebook"]:
        i["type"] = "social"
    elif i["about"]:
        if tokenPattern.match(i["about"]):
            i["type"] = "post_to"
        elif addressPattern.match(i["about"]):
            i["type"] = "post_to_simple"
        elif assetPattern.match(i["about"]):
            i["type"] = "post_club"
        else:
            i["type"] = "post_about"
    else:
        i["type"] = "regular"
