"""
Cryptoverse Root
================

Algorithm used by Cryptoverse

Version: 0.1.0.0

Example:

`ranking/cryptoverse_root <https://api.userfeeds.io/ranking/cryptoverse_root>`_

"""

ROOT_QUERY = """
SELECT claim.id, claim.target, claim.family, claim.sequence, claim.timestamp AS created_at, claim.author, claim.context, claim.about,
is_valid_erc721_context(claim.author, claim.context, claim.timestamp)
FROM persistent_claim AS claim
 WHERE (claim.target NOT LIKE 'claim:%%' OR claim.target IS NULL)
 AND (claim.about NOT LIKE 'claim:%%' OR claim.about IS NULL)
 ORDER BY created_at DESC
"""


def run(conn_mgr, input, **ignore):
    feed = fetch_feed(conn_mgr)
    for x in feed:
        if not x["is_valid_erc721_context"]:
            x["context"] = None
        del x["is_valid_erc721_context"]
    return {"items": feed}


def fetch_feed(conn_mgr):
    return conn_mgr.run_rdb(ROOT_QUERY, ())