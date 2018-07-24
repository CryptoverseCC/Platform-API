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
COALESCE(MAX(transfer.timestamp) OVER (PARTITION BY (claim.context, claim.timestamp, claim.target)) ,0) AS max_received,
COALESCE(MAX(transfer2.timestamp) OVER (PARTITION BY (claim.context, claim.timestamp, claim.target)),0) AS max_sent
FROM persistent_claim AS claim
 LEFT OUTER JOIN persistent_transfer AS transfer
 ON transfer.asset = SPLIT_PART(claim.context, ':', 1) || ':' || SPLIT_PART(claim.context, ':', 2)
 AND transfer.amount = SPLIT_PART(claim.context, ':', 3)
 AND  claim.author = transfer.receiver
 AND transfer.timestamp < claim.timestamp
LEFT OUTER JOIN persistent_transfer as transfer2
 ON transfer2.asset = SPLIT_PART(claim.context, ':', 1) || ':' || SPLIT_PART(claim.context, ':', 2)
 AND transfer2.amount = SPLIT_PART(claim.context, ':', 3)
 AND  claim.author = transfer2.senders
 AND transfer2.timestamp < claim.timestamp
 WHERE (claim.target NOT LIKE 'claim:%%' OR claim.target IS NULL)
 AND (claim.about NOT LIKE 'claim:%%' OR claim.about IS NULL)
 ORDER BY created_at DESC
"""


def run(conn_mgr, input, **ignore):
    feed = get_feed(conn_mgr)

    for x in feed:
        max_time_sent = x["max_sent"]
        max_time_rec = x["max_received"]
        if max_time_sent >= max_time_rec:
            x["context"] = None
        del x["max_received"]
        del x["max_sent"]
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
        "max_received" : feed[8],
        "max_sent": feed[9]
    }
