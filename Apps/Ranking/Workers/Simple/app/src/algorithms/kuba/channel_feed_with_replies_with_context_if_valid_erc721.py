"""
Channel feed with replies
=========================

Algorithm used by KUBA Chrome extension

Version: 0.1.0

Examples:

`ranking/kuba_channel_feed_with_replies_with_context_if_valid_erc721;id=test <https://api.userfeeds.io/ranking/kuba_channel_feed_with_replies_with_context_if_valid_erc721;id=test>`_

::
    curl -X POST \\
        -d '{"flow":[{"algorithm":"kuba_channel_feed_with_replies_with_context_if_valid_erc721","params":{"id":"https://userfeeds.github.io/cryptopurr/"}}]}' \\
        -H 'Content-Type: application/json' 'https://api.userfeeds.io/ranking/'

Json claim example:

.. code-block:: json

    {
        "id": "claim:0x464762e30e39458af1bfed2756adfdd3c673caefa4fb84544b21bbd90d03d262:0",
        "replies": [],
        "author": "0x460031ae4db5720d92a48fecf06a208c5099c186",
        "context": "ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:593163",
        "family": "kovan",
        "sequence": 6742075,
        "created_at": 1523000940000,
        "target": "Hurry up!"
    }
"""

from algorithms.utils import param

CHANNEL_FEED = """
MATCH
    (claim:Claim)-[:TARGET]->(target:Entity),
    (claim)<-[:AUTHORED]-(author:Identity),
    (claim)-[:IN]->(package:Package),
    (claim)-[:ABOUT]->(:Entity { id: {id} })
OPTIONAL MATCH
    (claim)-[:CONTEXT]->(context:Entity)
WHERE io.userfeeds.erc721.isValidClaim(claim)
OPTIONAL MATCH
    (claim)<-[:ABOUT]-(replyClaim:Claim),
    (replyClaim)-[:TARGET]->(replyTarget:Entity),
    (replyClaim)<-[:AUTHORED]-(replyAuthor:Identity),
    (replyClaim)-[:IN]->(replyPackage:Package)
WITH
    claim, target, author, package, context,
    replyClaim, replyTarget, replyAuthor, replyPackage,
    CASE replyClaim
        WHEN null THEN false
        ELSE io.userfeeds.erc721.isValidClaim(replyClaim) END AS erc721ValidClaim
OPTIONAL MATCH
    (replyClaim)-[:CONTEXT]->(replyContext:Entity)
WHERE erc721ValidClaim
RETURN
    claim.id AS id,
    target.id AS target,
    author.id AS author,
    package.family AS family,
    package.sequence AS sequence,
    package.timestamp AS created_at,
    context.id AS context,

    collect(replyClaim.id) AS reply_id,
    collect(replyTarget.id) AS reply_target,
    collect(replyAuthor.id) AS reply_author,
    collect(replyPackage.family) AS reply_family,
    collect(replyPackage.sequence) AS reply_sequence,
    collect(replyPackage.timestamp) AS reply_created_at,
    collect(erc721ValidClaim) AS reply_context_exists,
    collect(replyContext.id) AS reply_context
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    feed = fetch_feed(conn_mgr, params)
    mapped_feed = map_feed(feed)
    return {
        "items": mapped_feed
    }

def fetch_feed(conn_mgr, params):
    return conn_mgr.run_graph(CHANNEL_FEED, params)

def map_feed(feed):
    return [map_feed_item(feed_item) for feed_item in feed]

def map_feed_item(feed_item):
    return {
        "id": feed_item["id"],
        "target": feed_item["target"],
        "author": feed_item["author"],
        "family": feed_item["family"],
        "sequence": feed_item["sequence"],
        "created_at": feed_item["created_at"],
        "context": feed_item["context"],
        "replies": get_replies(feed_item),
    }

def get_replies(feed_item):
    reply_contexts = iter(feed_item["reply_context"])
    return [{
        "id": reply_id,
        "target": reply_target,
        "author": reply_author,
        "family": reply_family,
        "sequence": reply_sequence,
        "created_at": reply_created_at,
        "context": next(reply_contexts) if reply_context_exists else None,
    } for reply_id, reply_target, reply_author, reply_family, reply_sequence, reply_created_at, reply_context_exists in zip(
        feed_item["reply_id"],
        feed_item["reply_target"],
        feed_item["reply_author"],
        feed_item["reply_family"],
        feed_item["reply_sequence"],
        feed_item["reply_created_at"],
        feed_item["reply_context_exists"])
    ]
