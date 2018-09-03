"""
Unsupported balances
====================
"""

from algorithms.utils import param
from collections import defaultdict

ALL_BALANCES = """
MATCH (t)-[:RECEIVER]->(receiver:Identity { id: {identity} })
WHERE t.asset STARTS WITH 'ethereum:'
RETURN t.amount as amount, t.asset as asset
UNION ALL
MATCH (t)-[:SENDER]->(sender:Identity { id: {identity} })
WHERE t.asset STARTS WITH 'ethereum:'
RETURN '-' + t.amount as amount, t.asset as asset
"""


@param("identity", required=True)
@param("clubs", required=True)
def run(conn_mgr, input, **params):
    supported = set(params["clubs"])
    results = conn_mgr.run_graph(ALL_BALANCES, params)
    balances = defaultdict(int)
    for r in results:
        if not r["asset"] in supported:
            balances[r["asset"]] += int(float(r["amount"]))
    return {k: v for k, v in balances.items() if v > 0}
