"""
Cryptoverse Root
================

Algorithm used by Cryptoverse

Version: 0.1.0.0

Example:

`ranking/cryptoverse_root <https://api.userfeeds.io/ranking/cryptoverse_root>`_

"""

ROOT_QUERY = """
SELECT DISTINCT claim.id, claim.target, claim.family, claim.sequence, claim.timestamp AS created_at, claim.author, claim.context, claim.about,
is_valid_erc721_context(claim.author, SPLIT_PART(claim.context, ':', 1) || ':' || SPLIT_PART(claim.context, ':', 2),SPLIT_PART(claim.context, ':', 3),  claim.timestamp)
FROM persistent_claim AS claim
 WHERE (claim.target NOT LIKE 'claim:%%' OR claim.target IS NULL)
 AND (claim.about NOT LIKE 'claim:%%' OR claim.about IS NULL)
 ORDER BY created_at DESC
"""


def run(conn_mgr, input, **ignore):
    feed = get_feed(conn_mgr)

    for x in feed:
        is_valid_context = x["is_valid_erc721_context"]
        if not is_valid_context:
            x["context"] = None
            del x["is_valid_erc721_context"]
    return {"items": feed}


def get_feed(conn_mgr):
    feed = fetch_feed(conn_mgr)
    mapped = [map_feed_item(feed_item) for feed_item in feed]
    return mapped


def fetch_feed(conn_mgr):
    return conn_mgr.run_rdb(ROOT_QUERY, ())


def map_feed_item(feed):
    return {
        "id": feed[0],
        "target": feed[1],
        "family": feed[2],
        "sequence": feed[3],
        "created_at": feed[4],
        "author": feed[5],
        "context": feed[6],
        "about": feed[7],
        "is_valid_erc721_context" : feed[8]
    }
