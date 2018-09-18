"""
Tokntalk.club fan club token mint allowance
===========================================

"""

from algorithms.utils import materialize_records

ALLOWANCE_QUERY = """
MATCH (identity)-[:AUTHORED]->(c:Claim)-[:IN]->(p)
WITH DISTINCT p.timestamp / 68400000 as active, identity
RETURN count(active) * 100 as score, identity.id as id
ORDER BY score DESC
"""


def run(conn_mgr, input, **params):
    result = conn_mgr.run_graph(ALLOWANCE_QUERY, params)
    return {"items": materialize_records(result)}
