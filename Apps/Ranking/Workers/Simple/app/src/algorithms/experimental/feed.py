"""
Feed
====

Algorithm used by https://userfeeds.github.io/examples/examples/feeds/

Return information feeds for given ERC721 or ERC20 with proper amount

Version: 0.1.0

Example:

`ranking/experimental_feed;context=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:127 <https://api.userfeeds.io/ranking/experimental_feed;context=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:127>`_

Claim used by this algorithm looks like this:

.. code-block:: json

    {
      "claim": {
        "target": "I'm supercat! (true story)"
      },
      "context": "ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:341605"
    }

`context` is identifier of and ERC721 token you own
`claim.target` is an message you want to send

For examples of how to send information to Userfeeds Platform using web-components go to https://components.userfeeds.io/
"""

from algorithms.utils import param

FEED_QUERY = """
MATCH
    (target:Entity)<-[:TARGET]-(claim:Claim)-[:CONTEXT]->(context:Entity),
    (claim)<-[:AUTHORED]-(author:Entity:Identity),
    (claim)-[:IN]->(package:Entity:Package)
WHERE (context.id STARTS WITH {context}) AND io.userfeeds.erc721.isValidClaim(claim)
OPTIONAL MATCH
    (claim)<-[:TARGET]-(targetClaim:Claim),
    (targetClaim)-[:IN]->(targetPackage:Package),
    (targetClaim)<-[:AUTHORED]-(targetAuthor),
    (targetClaim)-[:CONTEXT]->(targetContext)
WHERE io.userfeeds.erc721.isValidClaim(targetClaim)
WITH
    claim,
    target,
    author,
    context,
    package,
    collect(targetClaim.id) as target_id,
    collect(targetAuthor.id) as target_author,
    collect(targetContext.id) as target_context,
    collect(targetPackage.family) as target_family,
    collect(targetPackage.sequence) as target_sequence,
    collect(targetPackage.timestamp) as target_created_at
RETURN
    claim.id as id,
    target.id as target,
    author.id as author,
    context.id as context,
    package.family as family,
    package.sequence as sequence,
    package.timestamp as created_at,

    target_id,
    target_author,
    target_context,
    target_family,
    target_sequence,
    target_created_at
"""


def map_feed_item(feed_item):
    return {
        "id": feed_item["id"],
        "target": feed_item["target"],
        "author": feed_item["author"],
        "context": feed_item["context"],
        "family": feed_item["family"],
        "sequence": feed_item["sequence"],
        "created_at": feed_item["created_at"],
    }


def map_feed(feed):
    return [map_feed_item(feed_item) for feed_item in feed]


def fetch_feed(conn_mgr, params):
    query_params = {"context": params["context"]}
    return conn_mgr.run_graph(FEED_QUERY, query_params)


def sort_by_created_at(query_result):
    return sorted(query_result, key=lambda x: x["created_at"], reverse=True)


@param("context", required=True)
def run(conn_mgr, input, **params):
    feed = fetch_feed(conn_mgr, params)
    mapped_feed = map_feed(feed)
    return {"items": sort_by_created_at(mapped_feed)}
