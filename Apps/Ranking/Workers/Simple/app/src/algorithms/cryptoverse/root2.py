"""
Cryptoverse Root
================

Algorithm used by Cryptoverse

Version: 0.1.0

Example:

`ranking/cryptoverse_root <https://api.userfeeds.io/ranking/cryptoverse_root>`_

"""

from algorithms.utils import group_by_function

ROOT_QUERY = """
SELECT id, target, family, sequence, timestamp as created_at, author, context, about
FROM persistent_claim claim
WHERE ((claim.target not like 'claim:%%' OR claim.target is null)
and (claim.about not like 'claim:%%' or claim.about is null))
ORDER BY created_at DESC
"""

RECEIVED = """
SELECT  claim.context, claim.timestamp as created_at, claim.author, COALESCE(max(transfer.timestamp),0) as max_timestamp
FROM persistent_claim as claim
INNER JOIN persistent_transfer as  transfer
ON transfer.asset = split_part(claim.context, ':', 1) || ':' || split_part(claim.context, ':', 2)
and transfer.amount = split_part(claim.context, ':', 3)
and transfer.receiver = claim.author
WHERE transfer.timestamp < claim.timestamp
and ((claim.target not like 'claim:%%' OR claim.target is null)
and (claim.about not like 'claim:%%' or claim.about is null))
GROUP BY (claim.context, claim.timestamp, claim.author)
"""

SENT = """
SELECT  claim.context, claim.timestamp as created_at, claim.author, COALESCE(max(transfer.timestamp),0) as max_timestamp
FROM persistent_claim as claim
INNER JOIN persistent_transfer as transfer
ON transfer.asset = split_part(claim.context, ':', 1) || ':' || split_part(claim.context, ':', 2)
and transfer.amount = split_part(claim.context, ':', 3)
and transfer.senders = claim.author
WHERE transfer.timestamp < claim.timestamp
and ((claim.target not like 'claim:%%' OR claim.target is null)
and (claim.about not like 'claim:%%' or claim.about is null))
GROUP BY (claim.context, claim.timestamp, claim.author)
"""


def run(conn_mgr, input, **ignore):
    feed = getFeed(conn_mgr)
    received = getTransfers(conn_mgr, RECEIVED)
    sent = getTransfers(conn_mgr, SENT)

    for x in feed:
        max_time_sent = get_max_timestamp_sent(sent, x)
        max_time_rec = get_max_timestamp_received(received, x)
        if max_time_sent >= max_time_rec:
            x["context"] = None
    return {"items": feed}


def get_max_timestamp_received(received, x):
    max_rec = received.get(create_cca_key(x))
    max_time_rec = max_timestamp_or_zero(max_rec)
    return max_time_rec


def get_max_timestamp_sent(sent, x):
    max_sent = sent.get(create_cca_key(x))
    max_time_sent = max_timestamp_or_zero(max_sent)
    return max_time_sent


def getFeed(conn_mgr):
    feed = fetch_feed(conn_mgr)
    mapped = [map_feed_item(feed_item) for feed_item in feed]
    return mapped


def getTransfers(conn_mgr, query):
    sent_result = conn_mgr.run_rdb(query, ())
    sent = group_by_function([map_transfer_item(transfer_item) for transfer_item in sent_result],
                             lambda x2: create_cca_key(x2))
    return sent


def create_cca_key(x):
    return xstr(x["context"]) + xstr(x["created_at"]) + x["author"]


def max_timestamp_or_zero(max_rec):
    if not max_rec:
        max_time_rec = 0
    else:
        max_time_rec = max_rec[0]["max_timestamp"]
    return max_time_rec


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


def map_transfer_item(transfer):
    return {
        "context": transfer[0],
        "created_at": transfer[1],
        "author": transfer[2],
        "max_timestamp": transfer[3]
    }


def xstr(s):
    if s is None:
        return ''
    return str(s)
