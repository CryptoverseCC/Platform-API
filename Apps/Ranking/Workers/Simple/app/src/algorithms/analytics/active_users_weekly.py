"""
Analytics Active Users Weekly
=============================
"""

from algorithms.utils import materialize_records

QUERY = """
MATCH (identity)-[:AUTHORED]->(c:Claim)-[:IN]->(p)
RETURN count(DISTINCT identity) AS count, p.timestamp / (7 * 24 * 60 * 60 * 1000) AS id
ORDER BY id DESC
"""


def run(conn_mgr, input, **ignore):
    results = conn_mgr.run_graph(QUERY, {})
    results = materialize_records(results)
    return {"items": results}
