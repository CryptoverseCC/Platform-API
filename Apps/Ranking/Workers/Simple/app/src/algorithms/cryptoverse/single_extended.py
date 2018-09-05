"""
Cryptoverse Single Extended
===========================

Allows to request multiple identities or tokens at once
"""

from algorithms.utils import param

MY_EXPRESSIONS_QUERY = """
MATCH (claim:Claim)-[:CONTEXT]->(context)
WHERE context.id IN {ids}
    AND io.userfeeds.erc721.isValidClaim(claim)
    AND NOT /*like*/ (claim)-[:TARGET]->(:Claim)
    AND NOT /*reply*/ (claim)-[:ABOUT]->(:Claim)
MATCH
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity)
OPTIONAL MATCH (claim)-[:ABOUT]->(about)
OPTIONAL MATCH (claim)-[labels:LABELS]->(target)
RETURN
    claim.id AS id,
    target.id AS target,
    package.family AS family,
    package.sequence AS sequence,
    package.timestamp AS created_at,
    identity.id as author,
    context.id as context,
    about.id as about,
    collect(labels.value) as labels
"""

EXPRESSIONS_ABOUT_ME_QUERY = """
MATCH (claim:Claim)-[:ABOUT]->(about)
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

EXPRESSIONS_TARGETING_ME_QUERY = """
MATCH (claim:Claim)-[:TARGET]->(target)
WHERE target.id IN {ids} AND NOT /*reply*/ (claim)-[:ABOUT]->(:Claim)
MATCH
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity)
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

MY_REACTIONS_QUERY = """
MATCH (claim:Claim)-[:CONTEXT]->(context),
    (claim)-[:TARGET]->(targetClaim:Claim)
WHERE context.id IN {ids}
    AND io.userfeeds.erc721.isValidClaim(claim)
    AND NOT /*reply or anything weird*/ (claim)-[:ABOUT]->()
MATCH
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity),
    (targetClaim)-[:TARGET]->(targetTarget),
    (targetClaim)-[:IN]->(targetPackage),
    (targetClaim)<-[:AUTHORED]-(targetIdentity)
OPTIONAL MATCH (targetClaim)-[:ABOUT]->(targetAbout)
WHERE NOT targetAbout:Claim
OPTIONAL MATCH (targetClaim)-[:CONTEXT]->(targetContext)
WHERE io.userfeeds.erc721.isValidClaim(targetClaim)
RETURN
    claim.id AS id,
    targetClaim.id AS target_id,
    targetTarget.id AS target_target,
    targetPackage.family AS target_family,
    targetPackage.sequence AS target_sequence,
    targetPackage.timestamp AS target_created_at,
    targetIdentity.id AS target_author,
    targetContext.id AS target_context,
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
    my = map_feed(fetch_feed(conn_mgr, MY_EXPRESSIONS_QUERY, ids))
    about_me = map_feed(fetch_feed(conn_mgr, EXPRESSIONS_ABOUT_ME_QUERY, ids))
    targeting_me = map_feed(fetch_feed(conn_mgr, EXPRESSIONS_TARGETING_ME_QUERY, ids))
    my_likes = map_likes(fetch_feed(conn_mgr, MY_REACTIONS_QUERY, ids))
    return {"items": my + about_me + targeting_me + my_likes}


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
        "context": feed_item["context"],
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
        "context": feed_item["context"],
    }
