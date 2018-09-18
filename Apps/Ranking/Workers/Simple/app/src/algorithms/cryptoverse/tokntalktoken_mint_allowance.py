"""
Tokntalk.club fan club token mint allowance
===========================================

"""

from algorithms.utils import param

ALLOWANCE_QUERY = """
MATCH (:Identity { id: {identity} })-[:AUTHORED]->(c)-[:IN]->(p)
WITH DISTINCT p.timestamp / 68400000 as active
RETURN count(active) * 100 as score
"""


@param("identity", required=True)
def run(conn_mgr, input, **params):
    result = conn_mgr.run_graph(ALLOWANCE_QUERY, params)
    return result.single()["score"]
