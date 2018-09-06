"""
Cryptoverse Notifications
=========================

"""

from algorithms.utils import param

EXPRESSIONS_ABOUT_ME_QUERY = """
MATCH (claim:Claim)-[:ABOUT]->(about:Entity)
WHERE about.id IN {ids} AND NOT /*like*/ (claim)-[:TARGET]->(:Claim)
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
"""

REACTIONS_ABOUT_ME_QUERY = """
MATCH (claim:Claim)-[:TARGET]->(targetClaim:Claim)<-[:AUTHORED]-(targetIdentity:Identity)
WHERE targetIdentity.id IN {ids}
    AND NOT io.userfeeds.erc721.isValidClaim(targetClaim)
    AND NOT /*reply or anything weird*/ (claim)-[:ABOUT]->()
MATCH
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity),
    (targetClaim)-[:TARGET]->(targetTarget),
    (targetClaim)-[:IN]->(targetPackage)
OPTIONAL MATCH (targetClaim)-[:ABOUT]->(targetAbout)
WHERE NOT targetAbout:Claim
OPTIONAL MATCH (claim)-[:CONTEXT]->(context)
WHERE io.userfeeds.erc721.isValidClaim(claim)
RETURN
    claim.id AS id,
    targetClaim.id AS target_id,
    targetTarget.id AS target_target,
    targetPackage.family AS target_family,
    targetPackage.sequence AS target_sequence,
    targetPackage.timestamp AS target_created_at,
    targetIdentity.id AS target_author,
    targetAbout.id AS target_about,
    package.family AS family,
    package.sequence AS sequence,
    package.timestamp AS created_at,
    identity.id AS author,
    context.id AS context
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    ids = params["id"]
    if isinstance(ids, str):
        ids = [ids]
    about_me = map_feed(fetch_feed(conn_mgr, EXPRESSIONS_ABOUT_ME_QUERY, ids))
    my_likes = map_likes(fetch_feed(conn_mgr, REACTIONS_ABOUT_ME_QUERY, ids))
    return {"items": about_me + my_likes}


def fetch_feed(conn_mgr, query, ids):
    return conn_mgr.run_graph(query, {"ids": ids})


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
        "context": feed_item.get("context"),
        "label": feed_item["labels"][0] if feed_item.get("labels") else None,
    }


def map_likes(feed):
    return [map_like_item(feed_item) for feed_item in feed]


def map_like_item(feed_item):
    return {
        "id": feed_item["id"],
        "target": {
            "id": feed_item["target_id"],
            "target": feed_item["target_target"],
            "author": feed_item["target_author"],
            "family": feed_item["target_family"],
            "sequence": feed_item["target_sequence"],
            "created_at": feed_item["target_created_at"],
            "about": feed_item["target_about"],
            "context": feed_item["target_context"],
        },
        "author": feed_item["author"],
        "family": feed_item["family"],
        "sequence": feed_item["sequence"],
        "created_at": feed_item["created_at"],
    }
