"""
Channel feed
============

Version: 0.1.0

Example:
::
    curl -X POST \\
        -d '{"flow":[{"algorithm":"experimental_channel_feed","params":{"id":"https://www.google.com"}}]}' \\
        -H 'Content-Type: application/json' 'https://api.userfeeds.io/ranking/'


Json claim example:

.. code-block:: json

    {
        "type":["about"],
        "claim":{
            "target":"Cool website, bro!",
            "about":"https://www.google.com"
        }
    }

ERC721 example:


`ranking/experimental_channel_feed;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:593163 <https://api.userfeeds.io/ranking/experimental_channel_feed;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:593163>`_

Json claim example:

.. code-block:: json

    {
        "type": ["about"],
        "claim": {
            "target": "New cool kitten on the block",
            "about": "ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:593163"
        },
        "context": "ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:608827"
    }
"""

from algorithms.utils import materialize_records
from algorithms.utils import param
from algorithms.utils import sort_by_created_at

FIND_CLAIMS_WITH_ABOUT_ID = """
MATCH
    (claim:Entity:Claim)-[:ABOUT]->(about:Entity { id: {id} }),
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
    query_result = conn_mgr.run_graph(FIND_CLAIMS_WITH_ABOUT_ID, params)
    mapped_items = materialize_records(query_result)
    return {"items": sort_by_created_at(mapped_items)}
