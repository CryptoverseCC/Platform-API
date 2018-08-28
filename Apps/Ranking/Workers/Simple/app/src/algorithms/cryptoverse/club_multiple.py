"""
Cryptoverse Club Multiple
=========================

"""

from algorithms.utils import param

ROOT_QUERY = """
MATCH (claim:Claim)-[:ABOUT]->(about:Entity)
WHERE about.id IN {ids} AND NOT /*like*/ (claim)-[:TARGET]->(:Claim)
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
    identity.id AS author,
    context.id AS context,
    about.id AS about
ORDER BY package.timestamp DESC
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    ids = params["id"]
    if isinstance(ids, str):
        ids = [ids]
    feed = fetch_feed(conn_mgr, ids)
    mapped_feed = map_feed(feed)
    return {"items": mapped_feed}


def fetch_feed(conn_mgr, ids):
    return conn_mgr.run_graph(ROOT_QUERY, {"ids": ids})


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
        "about": feed_item["about"],
    }
