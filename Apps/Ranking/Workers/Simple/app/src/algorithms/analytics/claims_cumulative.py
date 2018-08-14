"""
Analytics Active Users Cumulative
=================================
"""

QUERY = """
MATCH (:Claim)-[:IN]->(p)
RETURN p.timestamp AS id
ORDER BY id ASC
"""


def run(conn_mgr, input, **ignore):
    results = conn_mgr.run_graph(QUERY, {})
    results = [{"count": index + 1, "id": x["id"]} for index, x in enumerate(results)]
    return {"items": results}
