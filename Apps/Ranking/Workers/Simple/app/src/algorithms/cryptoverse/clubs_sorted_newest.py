"""
Cryptoverse Clubs Sorted
========================
"""

from algorithms.utils import materialize_records

CLUBS_QUERY = """
MATCH (club:Entity)<-[:ABOUT]-(claim)-[:IN]->(p)
WHERE club.id STARTS WITH 'ethereum:0x' AND club.id =~ 'ethereum:0x[0-9a-f]{40}'
RETURN club.id AS id, min(p.timestamp) AS score
ORDER BY score DESC
"""


def run(conn_mgr, input, **params):
    result = conn_mgr.run_graph(CLUBS_QUERY, {})
    result = materialize_records(result)
    return {"items": result}
