"""
Analytics Active Users Cumulative
=================================
"""

QUERY = """
MATCH (identity)-[:AUTHORED]->(c:Claim)-[:IN]->(p)
RETURN identity.id AS identity, min(p.timestamp) AS id
ORDER BY id ASC
"""


def run(conn_mgr, input, **ignore):
    results = conn_mgr.run_graph(QUERY, {})
    results = [{"count": index + 1, "id": x["id"]} for index, x in enumerate(results)]
    return {"items": results}
