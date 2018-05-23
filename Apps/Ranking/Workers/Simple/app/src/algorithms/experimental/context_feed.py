"""
Context feed
============

Version: 0.1.0

ERC721 example:

`ranking/experimental_context_feed;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:587035 <https://api.userfeeds.io/ranking/experimental_context_feed;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:587035>`_

Json claim example:

.. code-block:: json

    {
        "claim":{
            "target": "I love catnip"
        },
        "context": "ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:587035"
    }
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
