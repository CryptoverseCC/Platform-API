"""
Cryptoverse Nocoiners Simple
============================

"""

from algorithms.utils import tokenPattern, assetPattern, addressPattern
from algorithms.cryptoverse import nocoiners_simple
from algorithms.kuba import replies, reactions


def run(conn_mgr, input, **params):
    result = nocoiners_simple.run(conn_mgr, input, **params)
    result = replies.run(conn_mgr, result, nocoiners=True)
    result = reactions.run(conn_mgr, result, nocoiners=True)
    set_types(result["items"])
    return result


def set_types(items):
    for i in items:
        if tokenPattern.match(i["target"]) or addressPattern.match(i["target"]):
            i["type"] = "boost"
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
