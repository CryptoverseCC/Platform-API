"""
Payments
========

List of all claim targets made connected with given token sent to certain address.

Primarily used by Boost algorithm as a first step.

Version: 0.1.0

Example:

`ranking/experimental_payments;asset=ethereum;context=0x9816b4d0da6ae6204c0a8222ec47b0f33ff3f910 <https://api.userfeeds.io/ranking/experimental_payments;asset=ethereum;context=0x9816b4d0da6ae6204c0a8222ec47b0f33ff3f910>`_


Json claim example:

.. code-block:: json

    {
        "claim": {
            "target": "ceac98dd-41f1-4338-bd8e-aae20165cc82"
        }
    }
"""

from algorithms.utils import param


TARGET_WITH_SCORE = """
MATCH
    (claim:Claim)-[:TARGET]->(target:Entity),
    (claim)-[:CONNECTED_WITH]->(t:Transfer { asset: {asset} }),
    (t)-[:RECEIVER]->(:Identity { id: {context} })
RETURN
    target.id as id,
    t.amount as score
"""


@param("context", required=True)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    query_result = conn_mgr.run_graph(TARGET_WITH_SCORE, params)
    return {
        "items": [{
            "id": e["id"],
            "score": int(e["score"])
        } for e in query_result]
    }
