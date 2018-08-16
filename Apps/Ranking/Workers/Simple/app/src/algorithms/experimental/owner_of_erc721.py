"""
Owner of ERC721
===============
"""

from algorithms.utils import param, materialize_records

QUERY = """
UNWIND {contexts} AS context
WITH context, substring(context, 0, 51) AS asset, substring(context, 52) AS tokenId
MATCH (p)<-[:IN]-(:Transfer { asset: asset, amount: tokenId })-[:RECEIVER]->(identity)
WITH p.timestamp AS created_at, context, identity.id AS owner
ORDER BY created_at DESC
RETURN max(created_at) AS created_at, context, head(collect(owner)) AS owner
"""


@param("context", required=True)
def run(conn_mgr, input, **params):
    contexts = params["context"]
    if isinstance(contexts, str):
        contexts = [contexts]
    result = conn_mgr.run_graph(QUERY, {"contexts": contexts})
    result = materialize_records(result)
    return {"items": result}
