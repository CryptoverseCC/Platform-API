"""
Cryptoverse Club Last Week
==========================
"""

from algorithms.utils import param

ROOT_QUERY = """
MATCH (claim:Claim)-[:ABOUT]->(:Entity { id: {id} })
WHERE NOT /*like*/ (claim)-[:TARGET]->(:Claim)
MATCH
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity)
WHERE package.timestamp > timestamp() - 7 * 24 * 60 * 60 * 1000 // last week
OPTIONAL MATCH (claim)-[:CONTEXT]->(context)
WHERE io.userfeeds.erc721.isValidClaim(claim)
RETURN
    claim.id AS id,
    target.id AS target,
    package.family AS family,
    package.sequence AS sequence,
    package.timestamp AS created_at,
    identity.id as author,
    context.id as context
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
        "context": feed_item["context"],
    }
