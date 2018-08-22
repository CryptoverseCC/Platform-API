"""
Cryptoverse Clubs Sorted
========================

Algorithm used by Cryptoverse

Version: 0.1.0

Example:

`ranking/cryptoverse_clubs_sorted <https://api.userfeeds.io/ranking/cryptoverse_clubs_sorted>`_

"""

import jsonsempai
with jsonsempai.imports():
    import mapping
from algorithms.utils import materialize_records, param
import re

CLUBS_QUERY = """
MATCH (club:Entity)<-[:ABOUT]-(claim)
WHERE club.id IN {clubs}
RETURN club.id AS id, count(claim) as score
ORDER BY score DESC
"""


@param("clubs", required=False)
def run(conn_mgr, input, **params):
    if "clubs" in params:
        clubs = params["clubs"]
    else:
        clubs = [value.network + ":" + value.address for name, value in mapping.__dict__.items() if re.match("^[A-Z_]+$", name)]
    if isinstance(clubs, str):
        clubs = [clubs]
    result = conn_mgr.run_graph(CLUBS_QUERY, {"clubs": clubs})
    result = materialize_records(result)
    nonempty = [c["id"] for c in result]
    result.extend([{"id": c, "score": 0} for c in clubs if c not in nonempty])
    return {"items": result}
