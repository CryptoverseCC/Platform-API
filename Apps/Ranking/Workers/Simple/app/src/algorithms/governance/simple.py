"""
Governance Simple
=================

Algorithm returning base data for `Governance Enhanced <https://userfeeds.github.io/governance/>` app.

Version: 0.1.0

Example:

`ranking/governance_simple;identity=0x9816b4d0da6ae6204c0a8222ec47b0f33ff3f910 <https://api.userfeeds.io/ranking/governance_simple;identity=0x9816b4d0da6ae6204c0a8222ec47b0f33ff3f910>`_

"""
from algorithms.utils import param

QUERY = """
UNWIND {identities} AS identity
MATCH
    (n:Identity)-[:SENDER]-(t:Transfer)-[:IN]-(p:Package)
WHERE
    n.id = identity
RETURN identity, count(t) as transfers, collect(distinct t.asset) as assets, min(p.created_at) as since
"""


@param("identity", required=True)
def run(conn_mgr, input, **params):
    identities = params.get("identity")
    identities = [identities] if type(identities) == str else identities
    result = list(conn_mgr.run_graph(QUERY, {
        "identities": identities
    }))
    result = [{
        "identity": i["identity"],
        "transfers": i["transfers"],
        "assets": i["assets"],
        "since": i["since"],
    } for i in result]
    return {"items": result}
