"""
Contexts
========

Version: 0.1.0

ERC721 example:

`ranking/experimental_contexts;starts_with=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d <https://api.userfeeds.io/ranking/experimental_contexts;starts_with=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d>`_

Json claim example:

.. code-block:: json

    {
        "claim":{
            "target": "I love catnip"
        },
        "context": "ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:587035"
    }
"""

from algorithms.utils import param

FIND_CONTEXTS = """
MATCH
    (context:Entity)<-[:CONTEXT]-()
WHERE
    context.id STARTS WITH {starts_with}
RETURN
    distinct context.id as context
"""


@param("starts_with", required=True)
def run(conn_mgr, input, **params):
    query_result = conn_mgr.run_graph(FIND_CONTEXTS, params)
    return {"items": retrieve_contexts(query_result)}


def retrieve_contexts(query_result):
    return [{"id": record["context"]} for record in query_result]
