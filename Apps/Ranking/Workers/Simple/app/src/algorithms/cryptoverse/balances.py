"""
Unsupported balances
====================
"""

from algorithms.utils import param, addressPattern
from collections import defaultdict

ALL_BALANCES = """
MATCH (t)-[:RECEIVER]->(receiver:Identity { id: {entity} })
WHERE t.asset STARTS WITH 'ethereum:'
RETURN t.amount as amount, t.asset as asset
UNION ALL
MATCH (t)-[:SENDER]->(sender:Identity { id: {entity} })
WHERE t.asset STARTS WITH 'ethereum:'
RETURN '-' + t.amount as amount, t.asset as asset
"""

IDENTITY_FROM_CONTEXT = """
MATCH (identity)<-[:RECEIVER]-(:Transfer { asset: {erc721}, amount: {tokenId} })-[:IN]->(p:Package)
RETURN identity.id as identity
ORDER BY p.sequence DESC
LIMIT 1
"""


@param("entity", required=True)
def run(conn_mgr, input, **params):
    entity = params["entity"].lower()
    if not addressPattern.match(entity):
        erc721, token_id = entity.rsplit(":", 1)
        results = conn_mgr.run_graph(IDENTITY_FROM_CONTEXT, {"erc721": erc721, "tokenId": token_id})
        entity = results.single()["identity"]
    results = conn_mgr.run_graph(ALL_BALANCES, params)
    balances = defaultdict(int)
    for r in results:
        balances[r["asset"]] += int(float(r["amount"]))
    return {k: v for k, v in balances.items() if v > 0}
