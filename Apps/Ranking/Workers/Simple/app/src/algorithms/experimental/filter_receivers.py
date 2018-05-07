"""
WIP Filter receivers
====================

Version: 0.0.1

Filters input in the form of
```
{ "items" [
  { "address": "0xabc...", "something": "else" },
  { "address": "0x123...", "moar": "things" }
] }

to contain only elements that received certain asset.

"""

from algorithms.utils import param, pipeable, filter_debug


FILTERED_RECEIVERS = """
UNWIND {addresses} AS address
MATCH (i:Identity { id: address })
WHERE (i)<-[:RECEIVER]-({ asset: {asset} })
RETURN address
"""


@pipeable
@filter_debug
@param("asset", required=True)
def run(conn_mgr, input, asset, **ignore):
    addresses = [item["address"] for item in input["items"]]
    query_result = conn_mgr.run_graph(FILTERED_RECEIVERS, {"asset": asset, "addresses": addresses})
    return {
        "items": filter_items(input["items"], query_result)
    }

def filter_items(items, query_result):
    addresses = set([item["address"] for item in query_result])
    return [item for item in items if item["address"] in addresses]
