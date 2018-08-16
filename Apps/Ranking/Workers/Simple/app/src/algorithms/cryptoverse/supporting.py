"""
Supporting
==========

"""

import re
from algorithms.experimental import sort
from algorithms.filter import group
from algorithms.utils import param


BOOSTS = """
MATCH
    (claim:Claim)-[:TARGET]->(:Entity { id: {entity} }),
    (claim)-[:ABOUT]->(about:Entity),
    (claim)-[:CONNECTED_WITH]->(fee_transfer:Transfer { asset: {asset} }),
    (fee_transfer)-[:RECEIVER]->(:Identity { id: {fee_address} }),
    (claim)-[:CONNECTED_WITH]->(owner_transfer:Transfer { asset: {asset} }),
    (owner_transfer)-[:RECEIVER]->(owner:Identity),
    (claim)-[:IN]->(package:Package)
WHERE
    owner.id = about.id OR io.userfeeds.erc721.isContextOwner(owner.id, about.id, package.timestamp)
RETURN
    about.id as id,
    owner_transfer.amount as score,
    fee_transfer.amount as fee
"""

tokenPattern = re.compile("[a-z]+:0x[0-9a-f]{40}:\d+$")
addressPattern = re.compile("0x[0-9a-f]{40}")

@param("fee_address", required=True)
@param("entity", required=True)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    result = boosts(conn_mgr, **params)
    result = remove_non_matching_ids(result)
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

def remove_non_matching_ids(result):
    return  {
        "items": [item for item in result["items"] if tokenPattern.match(item["id"])]
    }

