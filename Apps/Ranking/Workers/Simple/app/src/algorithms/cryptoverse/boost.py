"""
Boost
=====

"""

from algorithms.experimental import sort
from algorithms.filter import group
from algorithms.utils import param


BOOSTS = """
MATCH
    (claim:Claim)-[:TARGET]->(target:Entity),
    (claim)-[:ABOUT]->(:Entity { id: {entity} }),
    (claim)-[:CONNECTED_WITH]->(fee_transfer:Transfer { asset: {asset} }),
    (fee_transfer)-[:RECEIVER]->(:Identity { id: {fee_address} }),
    (claim)-[:CONNECTED_WITH]->(owner_transfer:Transfer { asset: {asset} }),
    (owner_transfer)-[:RECEIVER]->(owner:Identity),
    (claim)-[:IN]->(package:Package)
WHERE
    owner.id = {entity} OR io.userfeeds.erc721.isContextOwner(owner.id, {entity}, package.timestamp)
RETURN
    target.id as id,
    owner_transfer.amount as score,
    fee_transfer.amount as fee
"""

@param("fee_address", required=True)
@param("entity", required=True)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    result = boosts(conn_mgr, **params)
    result = group.run(conn_mgr, result)
    result = sort.run(conn_mgr, result, by="score", order="desc")
    return result

def boosts(conn_mgr, **params):
    query_result = conn_mgr.run_graph(BOOSTS, params)
    return {
        "items": [{
            "id": e["id"],
            "context": e["id"],
            "score": int(e["score"]),
        } for e in query_result if int(e["fee"]) * 11 >= int(e["score"])]
    }
