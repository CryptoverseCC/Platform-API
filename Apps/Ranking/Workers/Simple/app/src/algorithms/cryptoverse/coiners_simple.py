"""
Cryptoverse Coiners Simple
==========================

Returns only those root messages where addresses received ether or a token at any point in time

"""

ROOT_QUERY = """
MATCH (claim:Claim)<-[:AUTHORED]-(identity)
WHERE /* coiner */ (identity)<-[:RECEIVER]-() AND NOT /*like*/ (claim)-[:TARGET]->(:Claim) AND NOT /*reply*/ (claim)-[:ABOUT]->(:Claim)
MATCH
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package)
OPTIONAL MATCH (claim)-[:ABOUT]->(about)
OPTIONAL MATCH (claim)-[:CONTEXT]->(context)
WHERE io.userfeeds.erc721.isValidClaim(claim)
OPTIONAL MATCH (claim)-[labels:LABELS]->(target)
RETURN
    claim.id AS id,
    target.id AS target,
    package.family AS family,
    package.sequence AS sequence,
    package.timestamp AS created_at,
    identity.id AS author,
    about.id AS about,
    context.id AS context,
    collect(labels.value) AS labels
ORDER BY package.timestamp DESC
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
        "label": feed_item["labels"][0] if feed_item.get("labels") else None,
    }
