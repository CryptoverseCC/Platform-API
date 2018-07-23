"""
Cryptoverse Root
================

Algorithm used by Cryptoverse

Version: 0.1.0

Example:

`ranking/cryptoverse_root <https://api.userfeeds.io/ranking/cryptoverse_root>`_

"""

ROOT_QUERY = """
SELECT id, target, family, sequence, timestamp as created_at, author, context, about
FROM persistent_claim claim
WHERE ((claim.target not like 'claim:%%' OR claim.target is null)
and (claim.about not like 'claim:%%' or claim.about is null))
ORDER BY created_at DESC
"""

RECEIVED = """
select COALESCE(max(transfer.timestamp),0) from persistent_transfer transfer
where transfer.asset = split_part(%(context)s, ':', 1) || ':' || split_part(%(context)s, ':', 2)
and transfer.amount = split_part(%(context)s, ':', 3)
and transfer.timestamp < %(created_at)s
and transfer.receiver = %(author)s
"""

SENT = """
select COALESCE(max(transfer.timestamp),0) from persistent_transfer transfer
where transfer.asset = split_part(%(context)s, ':', 1) || ':' || split_part(%(context)s, ':', 2)
and transfer.amount = split_part(%(context)s, ':', 3)
and transfer.timestamp < %(created_at)s
and transfer.senders = %(author)s
"""


def is_valid_erc721(conn_mgr, claim):
    return conn_mgr.run_rdb(RECEIVED, claim) >= conn_mgr.run_rdb(SENT, claim)


def run(conn_mgr, input, **ignore):
    feed = fetch_feed(conn_mgr)
    mapped = [map_feed_item(feed_item) for feed_item in feed]
    for x in mapped:
        if not is_valid_erc721(conn_mgr, x):
            del x["context"]
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
        "about": feed[7]
    }
