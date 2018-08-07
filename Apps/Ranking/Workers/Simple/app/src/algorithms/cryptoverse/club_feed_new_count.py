"""
Cryptoverse Club Feed New Messages Count
========================================
"""

from algorithms.utils import param

ROOT_QUERY = """
UNWIND {ids} as id
MATCH (p)<-[:IN]-(c:Claim)
WHERE ((c)-[:ABOUT]->({id: id[0]}) or (c)-[:ABOUT]->(:Claim)-[:ABOUT]->({id: id[0]})) AND p.timestamp > id[1]
RETURN id[0] as club_id, id[1] as version, count(c) as count
"""


@param("versions", required=True)
def run(conn_mgr, input, **params):
    ids = [[id, version] for id, version in params["versions"].items()]
    result = conn_mgr.run_graph(ROOT_QUERY, {"ids": ids})
    return {
        "items": [map(item) for item in result],
    }


def map(item):
    return {
        "club_id": item["club_id"],
        "version": item["version"],
        "count": item["count"],
    }
