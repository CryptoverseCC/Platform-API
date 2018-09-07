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
MATCH (claim:Claim)<-[:AUTHORED]-(identity:Identity { id: {id} })
WHERE NOT io.userfeeds.erc721.isValidClaim(claim)
    AND NOT /*like*/ (claim)-[:TARGET]->(:Claim)
    AND NOT /*reply*/ (claim)-[:ABOUT]->(:Claim)
MATCH
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package)
OPTIONAL MATCH (claim)-[:ABOUT]->(about)
OPTIONAL MATCH (claim)-[labels:LABELS]->(target)
RETURN
    claim.id AS id,
    target.id AS target,
    package.family AS family,
    package.sequence AS sequence,
    package.timestamp AS created_at,
    identity.id as author,
    about.id as about,
    collect(labels.value) as labels
"""

EXPRESSIONS_ABOUT_ME_QUERY = """
MATCH (claim:Claim)-[:ABOUT]->(about:Entity { id: {id} })
WHERE NOT /*like*/ (claim)-[:TARGET]->(:Claim)
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

MY_REACTIONS_QUERY = """
MATCH (claim:Claim)<-[:AUTHORED]-(identity:Identity { id: {id} }),
    (claim)-[:TARGET]->(targetClaim:Claim)
WHERE NOT io.userfeeds.erc721.isValidClaim(claim)
    AND NOT /*reply or anything weird*/ (claim)-[:ABOUT]->()
MATCH
    (claim)-[:IN]->(package),
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
    identity.id AS author
"""

MY_REPLIES_QUERY = """
MATCH (claim:Claim)<-[:AUTHORED]-(identity:Identity { id: {id} }),
    (claim)-[:ABOUT]->(aboutClaim:Claim)
WHERE NOT io.userfeeds.erc721.isValidClaim(claim)
    AND NOT /*like*/ (claim)-[:TARGET]->(:Claim)
MATCH
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package),
    (aboutClaim)-[:TARGET]->(aboutTarget),
    (aboutClaim)-[:IN]->(aboutPackage),
    (aboutClaim)<-[:AUTHORED]-(aboutIdentity)
OPTIONAL MATCH (aboutClaim)-[:ABOUT]->(aboutAbout)
WHERE NOT aboutAbout:Claim
OPTIONAL MATCH (aboutClaim)-[:CONTEXT]->(aboutContext)
WHERE io.userfeeds.erc721.isValidClaim(aboutClaim)
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
    identity.id AS author
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    id = params["id"]
    my = map_feed(fetch_feed(conn_mgr, MY_EXPRESSIONS_QUERY, id))
    about_me = map_feed(fetch_feed(conn_mgr, EXPRESSIONS_ABOUT_ME_QUERY, id))
    my_likes = map_likes(fetch_feed(conn_mgr, MY_REACTIONS_QUERY, id))
    my_replies = map_replies(fetch_feed(conn_mgr, MY_REPLIES_QUERY, id))
    return {"items": sorted(my + about_me + my_likes + my_replies, key=lambda x: x["created_at"], reverse=True)}


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
    }
