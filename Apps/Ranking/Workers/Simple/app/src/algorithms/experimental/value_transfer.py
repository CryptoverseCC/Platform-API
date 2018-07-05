"""
Value transfer
==============

Version: 0.1.0

Adds sum of value for asset sent to given receiver.

"""

from algorithms.utils import param, pipeable, filter_debug

VALUE_TRANSFER = """
MATCH (claim:Claim)
WHERE claim.id IN {ids}
OPTIONAL MATCH (claim)-[:CONNECTED_WITH]->(t1:Transfer { asset: {asset} })-[:RECEIVER]->(:Identity { id: {receiver} })
OPTIONAL MATCH (claim)<-[:TARGET]-(:Claim)-[:CONNECTED_WITH]->(t2:Transfer { asset: {asset} })-[:RECEIVER]->(:Identity { id: {receiver} })
RETURN
    claim.id AS id,
    t1.amount AS root_transfer,
    collect(t2.amount) AS boost_transfers
"""


@pipeable
@filter_debug
@param("asset", required=True)
@param("receiver", required=True)
def run(conn_mgr, input, **params):
    params["ids"] = [x["id"] for x in input["items"]]
    results = conn_mgr.run_graph(VALUE_TRANSFER, params)
    results = {r["id"]: sum([int(x) for x in r["boost_transfers"]]) + int(r["root_transfer"]) if r["root_transfer"] else 0 for r in results}
    for i in input["items"]:
        i["score"] = results[i["id"]]
    return {
        "items": sorted(input["items"], key=lambda x: int(x["score"]), reverse=True)
    }
