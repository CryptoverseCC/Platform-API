"""
Cryptoverse Last Week
=====================
"""

ROOT_QUERY = """
MATCH (claim:Claim)
WHERE NOT /*like*/ (claim)-[:TARGET]->(:Claim) AND NOT /*reply*/ (claim)-[:ABOUT]->(:Claim) AND NOT /*labels*/ (claim)-[:LABELS]->()
WITH claim
MATCH
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity)
WHERE package.timestamp > timestamp() - 7 * 24 * 60 * 60 * 1000 // last week
OPTIONAL MATCH (claim)-[:ABOUT]->(about)
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
"""


def run(conn_mgr, input, **ignore):
    feed = fetch_feed(conn_mgr)
    mapped_feed = map_feed(feed)
    return {"items": mapped_feed}


def fetch_feed(conn_mgr):
    return conn_mgr.run_graph(ROOT_QUERY, {})


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
