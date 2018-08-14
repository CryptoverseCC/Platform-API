"""
Analytics Active Users Daily
============================
"""

from algorithms.utils import materialize_records

QUERY = """
MATCH (identity)-[:AUTHORED]->(c:Claim)-[:IN]->(p)
RETURN count(DISTINCT identity) AS count, p.timestamp / (24 * 60 * 60 * 1000) AS id
ORDER BY id ASC
"""


def run(conn_mgr, input, **ignore):
    results = conn_mgr.run_graph(QUERY, {})
    results = materialize_records(results)
    return {"items": results}
