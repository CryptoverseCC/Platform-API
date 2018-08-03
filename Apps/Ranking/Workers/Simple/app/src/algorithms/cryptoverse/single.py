"""
Cryptoverse Single
==================

Algorithm used by Cryptoverse

Version: 0.1.0

Example:

`ranking/cryptoverse_single;id=ethereum:0x06012...a266d:341605 <https://api.userfeeds.io/ranking/cryptoverse_single;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:341605>`_

"""

from algorithms.utils import param

MY_EXPRESSIONS_QUERY = """
MATCH (claim:Claim)-[:CONTEXT]->(context:Entity { id: {id} })
WHERE io.userfeeds.erc721.isValidClaim(claim)
    AND NOT /*like*/ (claim)-[:TARGET]->(:Claim)
    AND NOT /*reply*/ (claim)-[:ABOUT]->(:Claim)
WITH claim, context
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
ORDER BY package.timestamp DESC
"""

EXPRESSIONS_ABOUT_ME_QUERY = """
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

MY_REACTIONS_QUERY = """
MATCH (claim:Claim)-[:CONTEXT]->(context:Entity { id: {id} }),
    (claim)-[:TARGET]->(targetClaim:Claim)
WHERE io.userfeeds.erc721.isValidClaim(claim)
    AND NOT /*reply or anything weird*/ (claim)-[:ABOUT]->()
WITH claim, context, targetClaim
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
ORDER BY package.timestamp DESC
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    my = map_feed(fetch_feed(conn_mgr, MY_EXPRESSIONS_QUERY, params["id"]))
    about_me = map_feed(fetch_feed(conn_mgr, EXPRESSIONS_ABOUT_ME_QUERY, params["id"]))
    my_likes = map_likes(fetch_feed(conn_mgr, MY_REACTIONS_QUERY, params["id"]))
    return {"items": sorted(my + about_me + my_likes, key=lambda x: x["created_at"], reverse=True)}


def fetch_feed(conn_mgr, query, id):
    return conn_mgr.run_graph(query, {"id": id})


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
