"""
Airdrop receivers
=================

Version: 0.1.0

Returns all addresses which historically received transfer connected with given airdrop id.

Example:
::
    curl -X POST \\
        -d '{"flow":[{"algorithm":"experimental_airdrop_receivers","params":{"id":"claim:0xb08b45fe956e1c0b31dfdf7d6f1007cb910799a77f4de2307a6a19a1f85a386c:0"}}]}' \\
        -H 'Content-Type: application/json' 'https://api.userfeeds.io/ranking/'

"""

from algorithms.utils import param

AIRDROP_RECEIVERS = """
MATCH (receiver:Identity)<-[:RECEIVER]-(t:Transfer)<-[:CONNECTED_WITH]-(:Claim)-[:TARGET]->(:Claim { id: {id} })
RETURN DISTINCT receiver.id as address, t.asset as asset, t.amount as amount
"""


@param("id", required=True)
def run(conn_mgr, input, id, **ignore):
    query_result = conn_mgr.run_graph(AIRDROP_RECEIVERS, {"id": id})
    return {
        "items": map_items(query_result)
    }


def map_items(query_result):
    return [{
        "address": item["address"],
        "amount": int(item["amount"])
    } for item in query_result]
