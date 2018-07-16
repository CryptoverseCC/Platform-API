"""
Assets balances
===============

Version: 0.1.0

Get balances of given assets for given identity.

"""

from algorithms.utils import param
from collections import defaultdict

ASSETS_BALANCES = """
MATCH (t)-[:RECEIVER]->(receiver:Identity { id: {identity} })
WHERE t.asset in {assets}
RETURN t.amount as amount, t.asset as asset
UNION ALL
MATCH (t)-[:SENDER]->(sender:Identity { id: {identity} })
WHERE t.asset in {assets}
RETURN '-' + t.amount as amount, t.asset as asset
"""


@param("identity", required=True)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    assets = params["asset"]
    if isinstance(assets, str):
        assets = [assets]
    params["assets"] = assets
    results = conn_mgr.run_graph(ASSETS_BALANCES, params)
    balances = defaultdict(int)
    for r in results:
        balances[r["asset"]] += int(r["amount"])
    return {k: v for k, v in balances.items() if v > 0}
