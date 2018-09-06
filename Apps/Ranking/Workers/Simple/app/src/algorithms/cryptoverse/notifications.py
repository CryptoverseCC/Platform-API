"""
Cryptoverse Notifications
===========================

All about me.
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

EXPRESSIONS_TARGETING_ME_QUERY = """
MATCH (claim:Claim)-[:TARGET]->(target:Entity)
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

REACTIONS_ABOUT_ME_QUERY = """
MATCH (claim:Claim)-[:TARGET]->(targetClaim:Claim)-[:CONTEXT]->(targetContext:Entity)
WHERE targetContext.id IN {ids}
    AND io.userfeeds.erc721.isValidClaim(targetClaim)
    AND NOT /*reply or anything weird*/ (claim)-[:ABOUT]->()
MATCH
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity),
    (targetClaim)-[:TARGET]->(targetTarget),
    (targetClaim)-[:IN]->(targetPackage),
    (targetClaim)<-[:AUTHORED]-(targetIdentity)
OPTIONAL MATCH (claim)-[:CONTEXT]->(context)
WHERE io.userfeeds.erc721.isValidClaim(claim)
OPTIONAL MATCH (targetClaim)-[:ABOUT]->(targetAbout)
WHERE NOT targetAbout:Claim
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

REPLIES_TO_ME_QUERY = """
MATCH (claim:Claim)-[:ABOUT]->(aboutClaim:Claim)-[:CONTEXT]->(aboutContext:Entity)
WHERE aboutContext.id IN {ids}
    AND io.userfeeds.erc721.isValidClaim(aboutClaim)
    AND NOT /*like*/ (claim)-[:TARGET]->(:Claim)
MATCH
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity),
    (aboutClaim)-[:TARGET]->(aboutTarget),
    (aboutClaim)-[:IN]->(aboutPackage),
    (aboutClaim)<-[:AUTHORED]-(aboutIdentity)
OPTIONAL MATCH (claim)-[:CONTEXT]->(context)
WHERE io.userfeeds.erc721.isValidClaim(claim)
OPTIONAL MATCH (aboutClaim)-[:ABOUT]->(aboutAbout)
WHERE NOT aboutAbout:Claim
RETURN
    claim.id AS id,
    aboutClaim.id AS about_id,
    aboutTarget.id AS about_target,
    aboutPackage.family AS about_family,
    aboutPackage.sequence AS about_sequence,
    aboutPackage.timestamp AS about_created_at,
    aboutIdentity.id AS about_author,
    aboutContext.id AS about_context,
    aboutAbout.id AS about_about,
    target.id AS target,
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
    targeting_me = map_feed(fetch_feed(conn_mgr, EXPRESSIONS_TARGETING_ME_QUERY, ids))
    my_likes = map_likes(fetch_feed(conn_mgr, REACTIONS_ABOUT_ME_QUERY, ids))
    replies_to_me = map_replies(fetch_feed(conn_mgr, REPLIES_TO_ME_QUERY, ids))
    return {"items": about_me + targeting_me + my_likes + replies_to_me}


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


def map_replies(feed):
    return [map_reply_item(feed_item) for feed_item in feed]


def map_reply_item(feed_item):
    return {
        "id": feed_item["id"],
        "reply_to": {
            "id": feed_item["about_id"],
            "target": feed_item["about_target"],
            "author": feed_item["about_author"],
            "family": feed_item["about_family"],
            "sequence": feed_item["about_sequence"],
            "created_at": feed_item["about_created_at"],
            "about": feed_item["about_about"],
            "context": feed_item["about_context"],
        },
        "target": feed_item["target"],
        "author": feed_item["author"],
        "family": feed_item["family"],
        "sequence": feed_item["sequence"],
        "created_at": feed_item["created_at"],
        "context": feed_item["context"],
    }
