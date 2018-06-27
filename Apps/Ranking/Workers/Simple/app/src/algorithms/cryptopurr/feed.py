"""
Feed
====

Algorithm used by https://userfeeds.github.io/cryptopurr/

Returns frontend specific structure of purrs.

Version: 0.1.1

Example:

`ranking/cryptopurr_feed;context=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d <https://api.userfeeds.io/ranking/cryptopurr_feed;context=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d>`_

Json claim example:

.. code-block:: json

    {
        "about": null,
        "abouted": [],
        "author": "0x460031ae4db5720d92a48fecf06a208c5099c186",
        "context": "ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:593163",
        "created_at": 1523000940000,
        "family": "kovan",
        "id": "claim:0x464762e30e39458af1bfed2756adfdd3c673caefa4fb84544b21bbd90d03d262:0",
        "sequence": 6742075,
        "target": {
            "id": "Hurry up!"
        },
        "targeted": [],
        "type": "regular"
    }
"""

from algorithms.utils import param

STARTS_WITH_ASSET = "STARTS WITH {context})"
EQUALS_CONTEXT = "= {context} OR (claim)-[:ABOUT]->(:Entity { id: {context} }))"
FEED_QUERY = """
MATCH
    (claim:Claim)-[:IN]->(package:Entity:Package),
    (claim)-[:CONTEXT]->(context:Entity)
WHERE {until} > package.timestamp > {since} AND (context.id %(context_filter)s %(id_filter)s
WITH claim, package, context
WHERE io.userfeeds.erc721.isValidClaim(claim) 
MATCH
    (target:Entity)<-[:TARGET]-(claim),
    (claim)<-[:AUTHORED]-(author:Entity:Identity)
WITH target, claim, context, package, author
OPTIONAL MATCH
    (claim)-[label:LABELS]->(target)
WITH
    claim,
    target,
    author,
    context,
    package,
    collect(label.value) as labels
OPTIONAL MATCH
    (claim)<-[:TARGET]-(targetClaim:Claim),
    (targetClaim)-[:IN]->(targetPackage:Package),
    (targetClaim)<-[:AUTHORED]-(targetAuthor:Identity),
    (targetClaim)-[:CONTEXT]->(targetContext:Entity)
WHERE targetContext.id STARTS WITH {asset} AND io.userfeeds.erc721.isValidClaim(targetClaim)
WITH
    claim,
    target,
    author,
    context,
    package,
    labels,
    collect(targetClaim.id) as target_id,
    collect(targetAuthor.id) as target_author,
    collect(targetContext.id) as target_context,
    collect(targetPackage.family) as target_family,
    collect(targetPackage.sequence) as target_sequence,
    collect(targetPackage.timestamp) as target_created_at
OPTIONAL MATCH
    (target)-[:IN]->(likePackage:Package),
    (target)<-[:AUTHORED]-(likeAuthor:Identity),
    (target)-[:CONTEXT]->(likeContext:Entity),
    (target)-[:TARGET]->(likeTarget:Entity)
WHERE likeContext.id STARTS WITH {asset} AND io.userfeeds.erc721.isValidClaim(target)
OPTIONAL MATCH
    (target)-[:IN]->(likeInvalidPackage:Package),
    (target)<-[:AUTHORED]-(likeInvalidAuthor:Identity),
    (target)-[:TARGET]->(likeInvalidTarget:Entity)
WHERE target:Claim
OPTIONAL MATCH
    (claim)-[:ABOUT]->(aboutedClaim:Claim),
    (aboutedClaim)-[:IN]->(aboutedPackage:Package),
    (aboutedClaim)<-[:AUTHORED]-(aboutedAuthor:Identity),
    (aboutedClaim)-[:CONTEXT]->(aboutedContext:Entity),
    (aboutedClaim)-[:TARGET]->(aboutedTarget:Entity)
WHERE aboutedContext.id STARTS WITH {asset} and io.userfeeds.erc721.isValidClaim(aboutedClaim)
OPTIONAL MATCH
    (claim)-[:ABOUT]->(aboutedInvalidClaim:Claim),
    (aboutedInvalidClaim)-[:IN]->(aboutedInvalidPackage:Package),
    (aboutedInvalidClaim)<-[:AUTHORED]-(aboutedInvalidAuthor:Identity),
    (aboutedInvalidClaim)-[:TARGET]->(aboutedInvalidTarget:Entity)
OPTIONAL MATCH
    (claim)-[:ABOUT]->(aboutedEntity:Entity)
WHERE aboutedEntity.id STARTS WITH {asset}
OPTIONAL MATCH
    (claim)-[:ABOUT]->(aboutedSomething:Entity)
OPTIONAL MATCH
    (claim)-[:TARGET]->(follow:Entity)
WHERE follow.id STARTS WITH {asset}
OPTIONAL MATCH
    (claim)<-[:ABOUT]-(aboutClaim:Claim),
    (aboutClaim)-[:IN]->(aboutPackage:Package),
    (aboutClaim)<-[:AUTHORED]-(aboutAuthor),
    (aboutClaim)-[:CONTEXT]->(aboutContext),
    (aboutClaim)-[:TARGET]->(aboutTarget)
WHERE aboutContext.id STARTS WITH {asset} AND io.userfeeds.erc721.isValidClaim(aboutClaim)
RETURN
    claim.id as id,
    target.id as target,
    author.id as author,
    context.id as context,
    package.family as family,
    package.sequence as sequence,
    package.timestamp as created_at,

    labels as labels,

    likeTarget.id as like_target,
    likeAuthor.id as like_author,
    likeContext.id as like_context,
    likePackage.family as like_family,
    likePackage.sequence as like_sequence,
    likePackage.timestamp as like_created_at,

    likeInvalidTarget.id as like_invalid_target,
    likeInvalidAuthor.id as like_invalid_author,
    likeInvalidPackage.family as like_invalid_family,
    likeInvalidPackage.sequence as like_invalid_sequence,
    likeInvalidPackage.timestamp as like_invalid_created_at,

    target_id,
    target_author,
    target_context,
    target_family,
    target_sequence,
    target_created_at,

    collect(aboutClaim.id) as about_id,
    collect(aboutTarget.id) as about_target_id,
    collect(aboutAuthor.id) as about_author,
    collect(aboutContext.id) as about_context,
    collect(aboutPackage.family) as about_family,
    collect(aboutPackage.sequence) as about_sequence,
    collect(aboutPackage.timestamp) as about_created_at,

    aboutedClaim.id as abouted_id,
    aboutedTarget.id as abouted_target_id,
    aboutedAuthor.id as abouted_author,
    aboutedContext.id as abouted_context,
    aboutedPackage.family as abouted_family,
    aboutedPackage.sequence as abouted_sequence,
    aboutedPackage.timestamp as abouted_created_at,

    aboutedInvalidClaim.id as abouted_invalid_id,
    aboutedInvalidTarget.id as abouted_invalid_target_id,
    aboutedInvalidAuthor.id as abouted_invalid_author,
    aboutedInvalidPackage.family as abouted_invalid_family,
    aboutedInvalidPackage.sequence as abouted_invalid_sequence,
    aboutedInvalidPackage.timestamp as abouted_invalid_created_at,

    aboutedEntity.id as abouted_entity,

    aboutedSomething.id as abouted_something,

    follow.id as follow
"""


def get_target(feed_item):
    if feed_item["like_target"]:
        return {
            "id": feed_item["target"],
            "target": {
                "id": feed_item["like_target"],
            },
            "author": feed_item["like_author"],
            "context": feed_item["like_context"],
            "family": feed_item["like_family"],
            "sequence": feed_item["like_sequence"],
            "created_at": feed_item["like_created_at"],
        }
    elif feed_item["like_invalid_target"]:
        return {
            "id": feed_item["target"],
            "target": {
                "id": feed_item["like_invalid_target"],
            },
            "author": feed_item["like_invalid_author"],
            "family": feed_item["like_invalid_family"],
            "sequence": feed_item["like_invalid_sequence"],
            "created_at": feed_item["like_invalid_created_at"],
        }
    else:
        return {
            "id": feed_item["target"],
        }


def get_about(feed_item):
    if feed_item["abouted_id"]:
        return {
            "id": feed_item["abouted_id"],
            "target": {
                "id": feed_item["abouted_target_id"],
            },
            "author": feed_item["abouted_author"],
            "context": feed_item["abouted_context"],
            "family": feed_item["abouted_family"],
            "sequence": feed_item["abouted_sequence"],
            "created_at": feed_item["abouted_created_at"],
        }
    if feed_item["abouted_invalid_id"]:
        return {
            "id": feed_item["abouted_invalid_id"],
            "target": {
                "id": feed_item["abouted_invalid_target_id"],
            },
            "author": feed_item["abouted_invalid_author"],
            "family": feed_item["abouted_invalid_family"],
            "sequence": feed_item["abouted_invalid_sequence"],
            "created_at": feed_item["abouted_invalid_created_at"],
        }
    if feed_item["abouted_entity"]:
        return {
            "id": feed_item["abouted_entity"],
        }
    if feed_item["abouted_something"]:
        return {
            "id": feed_item["abouted_something"],
        }
    return None


def get_targeted(feed_item):
    return [{
        "id": target_id,
        "author": target_author,
        "context": target_context,
        "family": target_family,
        "sequence": target_sequence,
        "created_at": target_created_at,
    } for target_id, target_author, target_context, target_family, target_sequence, target_created_at in
        zip(feed_item["target_id"],
            feed_item["target_author"],
            feed_item["target_context"],
            feed_item["target_family"],
            feed_item["target_sequence"],
            feed_item["target_created_at"])]


def get_abouted(feed_item):
    return [{
        "id": about_id,
        "target": {
            "id": about_target_id,
        },
        "author": about_author,
        "context": about_context,
        "family": about_family,
        "sequence": about_sequence,
        "created_at": about_created_at,
    } for about_id, about_target_id, about_author, about_context, about_family, about_sequence, about_created_at in
        zip(feed_item["about_id"],
            feed_item["about_target_id"],
            feed_item["about_author"],
            feed_item["about_context"],
            feed_item["about_family"],
            feed_item["about_sequence"],
            feed_item["about_created_at"])]


def get_type(feed_item):
    if feed_item["like_target"]:
        return "like"
    if feed_item["like_invalid_target"]:
        return "like_invalid"
    if feed_item["follow"]:
        return "follow"
    if feed_item["abouted_id"]:
        return "response"
    if feed_item["abouted_invalid_id"]:
        return "response_invalid"
    if feed_item["abouted_entity"]:
        return "post_to"
    if feed_item["abouted_something"]:
        return "post_about"
    if feed_item["labels"]:
        return "labels"
    return "regular"


def map_feed_item(feed_item):
    about = get_about(feed_item)
    targeted = get_targeted(feed_item)
    abouted = get_abouted(feed_item)
    target = get_target(feed_item)
    type = get_type(feed_item)
    return {
        "id": feed_item["id"],
        "target": target,
        "author": feed_item["author"],
        "context": feed_item["context"],
        "family": feed_item["family"],
        "sequence": feed_item["sequence"],
        "created_at": feed_item["created_at"],
        "labels": feed_item["labels"],
        "about": about,
        "targeted": targeted,
        "abouted": abouted,
        "type": type,
    }


def map_feed(feed):
    return [map_feed_item(feed_item) for feed_item in feed]


def fetch_feed(conn_mgr, params):
    context = params["context"]
    query_params = {"context": context}
    query_options = {}
    if "since" in params and "until" in params:
        query_params["until"] = int(params["until"])
        query_params["since"] = int(params["since"])
    else:
        query_params["until"] = 9223372036854775807
        query_params["since"] = 0
    if context.count(':') == 2:
        asset, id = context.rsplit(":", 1)
        query_params["asset"] = asset
        query_options["context_filter"] = EQUALS_CONTEXT
    else:
        query_params["asset"] = context
        query_options["context_filter"] = STARTS_WITH_ASSET
    if "id" in params:
        query_params["id"] = params["id"]
        query_options["id_filter"] = " AND claim.id = {id}"
    else:
        query_options["id_filter"] = ""
    return conn_mgr.run_graph(FEED_QUERY % query_options, query_params)


def sort_by_created_at(query_result):
    return sorted(query_result, key=lambda x: x["created_at"], reverse=True)


@param("until", required=False)
@param("since", required=False)
@param("context", required=True)
@param("full", required=False)
@param("id", required=False)
def run(conn_mgr, input, **params):
    feed = fetch_feed(conn_mgr, params)
    filtered_feed = filter_feed(feed, params)
    mapped_feed = map_feed(filtered_feed)
    return {"items": sort_by_created_at(mapped_feed)}

def filter_feed(feed, params):
    context = params["context"]
    if "full" in params:
        return feed
    elif context.count(':') == 2 or "id" in params:
        return feed
    else:
        return [feed_item for feed_item in feed if not feed_item["like_target"]]
