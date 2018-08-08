"""
Cryptoverse Single
==================

Algorithm used by Cryptoverse

Version: 0.1.0

Example:

`ranking/cryptoverse_single;id=ethereum:0x06012...a266d:341605 <https://api.userfeeds.io/ranking/cryptoverse_single;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:341605>`_

"""

from algorithms.utils import param

MY_EXPRESSIONS_QUERY = """
SELECT id, target, family, sequence, "timestamp" as created_at, author, context, about, labels
from persistent_claim where context = %(id)s and is_valid_erc721_context(author,%(_asset)s,%(_amount)s, "timestamp")
"""
EXPRESSIONS_ABOUT_ME_QUERY = """
SELECT id, target, family, sequence, "timestamp" as created_at, author, context, about, labels
from persistent_claim where about = %(id)s and is_valid_erc721_context(author,%(_asset)s,%(_amount)s, "timestamp")
"""
LIKES_TARGETS = """
SELECT id, target, family, sequence, "timestamp" as created_at, author, context, about, labels
from persistent_claim where id in (SELECT * FROM UNNEST(%(ids)s)) and is_valid_erc721_context(author,SPLIT_PART(context, ':', 1) || ':' || SPLIT_PART(context, ':', 2),SPLIT_PART(context, ':', 3), "timestamp")
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    _asset, _amount = params["id"].rsplit(":", 1)
    params = {"id": params["id"], "_asset": _asset, "_amount": _amount}

    myWithLikes = map_feed(conn_mgr.run_rdb(MY_EXPRESSIONS_QUERY, params))
    my = [claim for claim in myWithLikes if not claim["target"].startswith("claim:") and (not claim["about"] or not claim["about"].startswith("claim:"))]

    about_me = map_feed(conn_mgr.run_rdb(EXPRESSIONS_ABOUT_ME_QUERY, params))

    likes = [claim for claim in myWithLikes if claim["target"].startswith("claim:")]
    targets = [claim["target"] for claim in likes]
    likeTargets = map_targets(conn_mgr.run_rdb(LIKES_TARGETS, {"ids": targets}))

    my_likes = [add_target(like, likeTargets) for like in likes]
    return {"items": sorted(my + about_me + my_likes, key=lambda x: x["created_at"], reverse=True)}


def map_feed(feed):
    return [map_feed_item(feed_item) for feed_item in feed]

def map_targets(feed):
    return {feed_item["id"]: map_feed_item(feed_item) for feed_item in feed}


def map_feed_item(feed_item):
    return {
        "id": feed_item["id"],
        "target": feed_item["target"],
        "author": feed_item["author"],
        "family": feed_item["family"],
        "sequence": feed_item["sequence"],
        "created_at": feed_item["created_at"],
        "about": feed_item["about"],
        "context": feed_item["context"],
        "label": feed_item["labels"][0] if feed_item.get("labels") else None,
    }


def add_target(like, all_targets):
    target = all_targets[like["target"]]
    like["target"] = target
    del like["about"]
    del like["label"]
    if target.get("about") and target["about"].startswith("claim:"):
        target["about"] = None
    try:
        del target["label"]
    except KeyError:
        pass
    return like