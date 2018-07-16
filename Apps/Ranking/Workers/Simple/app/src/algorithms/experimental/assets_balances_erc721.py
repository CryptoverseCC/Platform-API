"""
Assets balances erc721
======================

Version: 0.1.0

Get balances of given assets for given context (erc721).

"""

from algorithms.experimental import assets_balances
from algorithms.utils import param

IDENTITY_FROM_CONTEXT = """
MATCH (identity)<-[:RECEIVER]-(:Transfer { asset: {erc721}, amount: {tokenId} })-[:IN]->(p:Package)
RETURN identity.id as identity
ORDER BY p.sequence DESC
LIMIT 1
"""


@param("context", required=True)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    erc721, token_id = params["context"].rsplit(":", 1)
    results = conn_mgr.run_graph(IDENTITY_FROM_CONTEXT, {"erc721": erc721, "tokenId": token_id})
    params["identity"] = results.single()["identity"]
    return assets_balances.run(conn_mgr, input, **params)
