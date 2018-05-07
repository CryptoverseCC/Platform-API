"""
With transfer to
================

Version: 0.1.0

"""

from algorithms.utils import param

CLAIMS_WITH_TRANSFER_TO = """
Match (c:Claim)-[:CONNECTED_WITH]->(t:Transfer { asset: { asset } })-[:RECEIVER]->(:Identity { id: { identity } })
Match (c)-[:TARGET]->(target:Entity)
Match (t)-[:SENDER]->(sender:Identity)
Return
    c.id as id,
    target.id as target,
    sender.id as sender,
    t.amount as amount
"""


@param("asset", required=True)
@param("identity", required=True)
def run(conn_mgr, input, **params):
    results = conn_mgr.run_graph(CLAIMS_WITH_TRANSFER_TO, params)
    items = [
        {
            "id": item["id"],
            "target": item["target"],
            "sender": item["sender"],
            "amount": item["amount"],
        } for item in results
    ]
    return {"items": items}
