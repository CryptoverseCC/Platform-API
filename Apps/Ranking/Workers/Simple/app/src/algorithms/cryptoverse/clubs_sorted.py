"""
Cryptoverse Clubs Sorted
========================

Algorithm used by Cryptoverse

Version: 0.1.0

Example:

`ranking/cryptoverse_clubs_sorted <https://api.userfeeds.io/ranking/cryptoverse_clubs_sorted>`_

"""

from algorithms.utils import materialize_records, param

CLUBS_QUERY = """
MATCH (club:Entity)<-[:ABOUT]-(claim)
WHERE club.id STARTS WITH 'ethereum:0x' AND club.id =~ 'ethereum:0x[0-9a-f]{40}'
RETURN club.id AS id, count(claim) AS score
ORDER BY score DESC
"""


@param("clubs", required=False)
def run(conn_mgr, input, **params):
    clubs = params.get("clubs", [])
    result = conn_mgr.run_graph(CLUBS_QUERY, {})
    result = materialize_records(result)
    nonempty = [c["id"] for c in result]
    result.extend([{"id": c, "score": 0} for c in clubs if c not in nonempty])
    return {"items": result}
