"""
Cryptoverse Thread
==================

Algorithm used by Cryptoverse

Version: 0.1.0

Example:

`ranking/cryptoverse_thread;id=claim:0x5bad1020a...6a9b13301b <https://api.userfeeds.io/ranking/cryptoverse_thread;id=claim:0x5bad1020a14a58f358363c35eb4fa3d3eb3b1c58c160196e810e77771205444e7b7336e9c42b99960f189e8e828c9643f6fcf4e42233a0473a5570ee6a9b13301b>`_

"""

from algorithms.utils import param
from algorithms.kuba import replies, replies_count

ROOT_QUERY = """
MATCH
    (claim:Claim)
WHERE claim.id IN {ids}
MATCH
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(identity)
OPTIONAL MATCH (claim)-[:ABOUT]->(about)
OPTIONAL MATCH
    (about)-[:TARGET]->(aboutTarget),
    (about)-[:IN]->(aboutPackage),
    (about)<-[:AUTHORED]-(aboutIdentity)
OPTIONAL MATCH (about)-[:ABOUT]->(aboutAbout)
WHERE NOT aboutAbout:Claim
OPTIONAL MATCH (about)-[:CONTEXT]->(aboutContext)
WHERE io.userfeeds.erc721.isValidClaim(about)
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
    about.id as about,
    aboutTarget.id as about_target,
    aboutPackage.family AS about_family,
    aboutPackage.sequence AS about_sequence,
    aboutPackage.timestamp AS about_created_at,
    aboutIdentity.id AS about_author,
    aboutContext.id AS about_context,
    aboutAbout.id AS about_about
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    root = map_feed(fetch_feed(conn_mgr, params["id"]))
    add_replies(conn_mgr, root, 5)
    return {"items": root}


def fetch_feed(conn_mgr, id):
    if isinstance(id, str):
        ids = [id]
    else:
        ids = id
    return conn_mgr.run_graph(ROOT_QUERY, {"ids": ids})


def map_feed(feed):
    return [map_feed_item(feed_item) for feed_item in feed]


def map_feed_item(feed_item):
    reply_to = None
    if feed_item["about_target"]:
        reply_to = {
            "target": feed_item["about_target"],
            "family": feed_item["about_family"],
            "sequence": feed_item["about_sequence"],
            "created_at": feed_item["about_created_at"],
            "author": feed_item["about_author"],
            "context": feed_item["about_context"],
            "about": feed_item["about_about"],
        }
    return {
        "id": feed_item["id"],
        "target": feed_item["target"],
        "author": feed_item["author"],
        "family": feed_item["family"],
        "sequence": feed_item["sequence"],
        "created_at": feed_item["created_at"],
        "about": feed_item["about"],
        "context": feed_item["context"],
        "reply_to": reply_to,
    }


def add_replies(conn_mgr, parents, depth):
    if depth == 0:
        replies_count.run(conn_mgr, {"items": parents})
    else:
        replies.run(conn_mgr, {"items": parents})
        children = [reply for parent in parents for reply in parent["replies"]]
        add_replies(conn_mgr, children, depth - 1)
