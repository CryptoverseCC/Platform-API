"""
Latest purrers
==============

Version: 0.1.0

"""

from algorithms.utils import materialize_records, sort_by_created_at

LATEST_PURRERS = """
MATCH (author)-[:AUTHORED]->(claim:Claim)-[:IN]->(package)
WHERE package.timestamp > timestamp() - 604800000 // last week
OPTIONAL MATCH (claim)-[:CONTEXT]->(context)
WHERE io.userfeeds.erc721.isValidClaim(claim)
RETURN
    author.id AS author,
    context.id AS context,
    max(package.timestamp) AS created_at
"""


def run(conn_mgr, input, **params):
    query_result = conn_mgr.run_graph(LATEST_PURRERS, params)
    mapped_items = materialize_records(query_result)
    return {"items": sort_by_created_at(mapped_items)}
