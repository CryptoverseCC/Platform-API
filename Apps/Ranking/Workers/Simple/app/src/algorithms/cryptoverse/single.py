"""
Cryptoverse Single
==================

Algorithm used by Cryptoverse

Version: 0.1.0

Example:

`ranking/cryptoverse_single;id=ethereum:0x06012...a266d:341605 <https://api.userfeeds.io/ranking/cryptoverse_single;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:341605>`_

"""

from algorithms.utils import param

SINGLE_QUERY = """
MATCH (claim:Claim)-[:CONTEXT]->(context:Entity { id: {id} })
WHERE io.userfeeds.erc721.isValidClaim(claim) AND NOT /*like*/ (claim)-[:TARGET]->(:Claim) AND NOT /*reply*/ (claim)-[:ABOUT]->(:Claim)
WITH claim, context
MATCH
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity)
OPTIONAL MATCH (claim)-[:ABOUT]->(about)
RETURN
    claim.id AS id,
    target.id AS target,
    package.family AS family,
    package.sequence AS sequence,
    package.timestamp AS created_at,
    identity.id as author,
    context.id as context,
    about.id as about
ORDER BY package.timestamp DESC
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    feed = fetch_feed(conn_mgr, params["id"])
    mapped_feed = map_feed(feed)
    return {"items": mapped_feed}


def fetch_feed(conn_mgr, id):
    return conn_mgr.run_graph(SINGLE_QUERY, {"id": id})


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
        "about": feed_item["about"],
        "context": feed_item["context"],
    }
