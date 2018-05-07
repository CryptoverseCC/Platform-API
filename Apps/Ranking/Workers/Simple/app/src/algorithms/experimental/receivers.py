"""
Receivers
=========

Version: 0.1.0

Returns receivers of given asset since given timestamp but not older than one day.
Timestamp have to be in milliseconds.

Example:
::
    curl -X POST \\
        -d '{"flow":[{"algorithm":"experimental_receivers","params":{"timestamp":1523450030000,"asset":"ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d"}}]}' \\
        -H 'Content-Type: application/json' 'https://api.userfeeds.io/ranking/'

"""

import time
from algorithms.utils import param

FIND_CLAIMS_WITH_ABOUT_ID = """
MATCH (p:%s)
WHERE p.created_at > {timestamp}
WITH p, 1 as dummy
MATCH (p)<-[:IN]-(:Transfer {asset: {asset}})-[:RECEIVER]->(identity)
RETURN distinct identity.id as address, max(p.created_at) as timestamp
"""


@param("asset", required=True)
@param("timestamp", required=False)
def run(conn_mgr, input, **params):
    timestamp = limit_timestamp(params)
    query = get_query_for_family(params)
    query_result = conn_mgr.run_graph(query, {"asset": params["asset"], "timestamp": timestamp})
    return {"items": map_items(query_result)}


def limit_timestamp(params):
    current_time = time.time() * 1000
    milliseconds_in_day = 24 * 60 * 60 * 1000
    oldest_timestamp_allowed = current_time - milliseconds_in_day
    if "timestamp" in params:
        timestamp = params["timestamp"]
        return max(timestamp, oldest_timestamp_allowed)
    else:
        return oldest_timestamp_allowed


def get_query_for_family(params):
    family_label_map = {
        "ethereum": "PackageEthereum",
        "rinkeby": "PackageRinkeby",
        "ropsten": "PackageRopsten",
        "kovan": "PackageKovan",
    }
    family = params["asset"].split(":")[0]
    return FIND_CLAIMS_WITH_ABOUT_ID % family_label_map[family]


def map_items(query_result):
    return [{
        "address": item["address"],
        "timestamp": item["timestamp"],
    } for item in query_result]
