"""
Receivers
=========

Version: 0.1.0

Returns all addresses which historically received given asset.

Example:
::
    curl -X POST \\
        -d '{"flow":[{"algorithm":"experimental_all_receivers","params":{"asset":"ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d"}}]}' \\
        -H 'Content-Type: application/json' 'https://api.userfeeds.io/ranking/'

"""

from algorithms.utils import param

ALL_RECEIVERS = """
MATCH (t:Transfer { asset: {asset} })-[:RECEIVER]->(i)
RETURN DISTINCT i.id as address
"""


@param("asset", required=True)
def run(conn_mgr, input, **params):
    if ':' not in params["asset"]:
        raise Exception("Asset needs to be a token (containing ':')")
    query_result = conn_mgr.run_graph(ALL_RECEIVERS, params)
    return {
        "items": map_items(query_result)
    }

def map_items(query_result):
    return [{
        "address": item["address"]
    } for item in query_result]
