"""
Cryptoverse Club
================

Algorithm used by Cryptoverse

Version: 0.1.0

Example:

`ranking/cryptoverse_club <https://api.userfeeds.io/ranking/cryptoverse_root>`_

"""

from algorithms.utils import param

ROOT_QUERY = """
MATCH (claim:Claim)-[:ABOUT]->(about:Entity { id: {id} })
WHERE NOT /*like*/ (claim)-[:TARGET]->(:Claim)
WITH claim, about
MATCH
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity)
OPTIONAL MATCH (claim)-[:CONTEXT]->(context)
WHERE io.userfeeds.erc721.isValidClaim(claim)
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
    return conn_mgr.run_graph(ROOT_QUERY, {"id": id})


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
