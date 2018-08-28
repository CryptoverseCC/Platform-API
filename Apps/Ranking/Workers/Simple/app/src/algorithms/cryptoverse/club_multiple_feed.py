"""
Cryptoverse club multiple feed with replies and likes
=====================================================

The difference between this and club_feed is that this returns about: "club id" and type is "post_club" for root messages

"""

from algorithms.utils import assetPattern, tokenPattern, addressPattern
from algorithms.cryptoverse import club_multiple
from algorithms.kuba import replies, reactions
from algorithms.utils import param


@param("id", required=True)
def run(conn_mgr, input, **params):
    result = club_multiple.run(conn_mgr, input, **params)
    result = replies.run(conn_mgr, result)
    result = reactions.run(conn_mgr, result)
    set_types(result["items"])
    return result


def set_types(items):
    for i in items:
        if tokenPattern.match(i["target"]) or addressPattern.match(i["target"]):
            i["type"] = "boost"
        elif i.get("about"):
            if assetPattern.match(i["about"]):
                i["type"] = "post_club"
            elif tokenPattern.match(i["about"]):
                i["type"] = "post_to"
            elif addressPattern.match(i["about"]):
                i["type"] = "post_to_simple"
            else:
                i["type"] = "post_about"
        else:
            i["type"] = "regular"
