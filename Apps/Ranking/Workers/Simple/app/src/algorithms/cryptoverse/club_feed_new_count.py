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
    result = [map_item(item) for item in result]
    result_map = {item["club_id"]: {item["version"]: item["count"]} for item in result}
    result.extend([{"club_id": id, "version": version, "count": 0} for id, version in ids if not result_map.get(id) or not result_map.get(id).get(version)])
    return {
        "items": result,
    }


def map_item(item):
    return {
        "club_id": item["club_id"],
        "version": item["version"],
        "count": item["count"],
    }
