"""
Context feed
============

Algorithm used by https://userfeeds.github.io/examples/examples/feeds/

Return information feeds for given ERC721 or ERC20 with proper amount

Version: 0.1.0

Example:

`ranking/experimental_context_feed;context=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:134330 <https://api.userfeeds.io/ranking/experimental_context_feed;context=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:134330>`_

Claim used by this algorithm looks like this:

.. code-block:: json

    {
      "claim": {
        "target": "I'm supercat! (true story)"
      },
      "context": "ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:341605"
    }

`context` is identifier of and ERC721 token you own
`claim.target` is an mes
sage you want to send

For examples of how to send information to Userfeeds Platform using web-components go to https://components.userfeeds.io/
"""


from algorithms.utils import materialize_records
from algorithms.utils import param
from algorithms.utils import sort_by_created_at

FIND_CLAIMS_IN_CONTEXT = """
MATCH
    (claim:Entity:Claim)-[:CONTEXT]->(context:Entity { id: {id} }),
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package:Entity:Package),
    (claim)<-[:AUTHORED]-(author:Entity:Identity)
RETURN
    claim.id as id,
    target.id as target,
    author.id as author,
    package.family as family,
    package.sequence as sequence,
    package.timestamp as created_at
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    query_result = conn_mgr.run_graph(FIND_CLAIMS_IN_CONTEXT, params)
    mapped_items = materialize_records(query_result)
    return {"items": sort_by_created_at(mapped_items)}
