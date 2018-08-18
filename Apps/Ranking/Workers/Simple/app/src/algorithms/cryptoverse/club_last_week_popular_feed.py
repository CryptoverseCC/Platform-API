"""
Cryptoverse club last week feed with replies and likes
======================================================
"""

from algorithms.utils import tokenPattern, addressPattern
from algorithms.cryptoverse import club_last_week
from algorithms.kuba import replies, reactions
from algorithms.utils import param


@param("id", required=True)
def run(conn_mgr, input, **params):
    result = club_last_week.run(conn_mgr, input, **params)
    result = replies.run(conn_mgr, result)
    result = reactions.run(conn_mgr, result)
    items = result["items"]
    set_score(items)
    set_types(items)
    items = sorted(items, key=lambda x: x["score"], reverse=True)
    return {"items": items}


def set_score(items):
    for i in items:
        addresses = set([i["author"]])
        for l in i["likes"]:
            addresses.add(l["author"])
        for r in i["replies"]:
            addresses.add(r["author"])
            for rl in r["likes"]:
                addresses.add(rl["author"])
        i["score"] = len(addresses)


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
