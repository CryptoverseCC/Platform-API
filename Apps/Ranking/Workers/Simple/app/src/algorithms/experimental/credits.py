"""
Credits
==========

Returns list of credits value that were present in claim.credits along with number of time they were there. For a given `type` parameter


Version: 0.2.0

"""
from algorithms.utils import param

CLAIMS_WITH_CREDITS = """
MATCH 
    (c:Claim)-[:IN]->(p:Package),
    (c)-[:TARGET]->(target:Entity),
    (c)-[credit:GIVES_CREDITS]->(e:Entity),
    (c)<-[:AUTHORED]-(author:Identity)
WHERE
    credit.type = {type}
RETURN
    distinct(e.id) AS value,
    count(e) as score
"""


def map_item(item):
    return {
        "value": item["value"],
        "score": item["score"],
    }


@param("type", required=True)
def run(conn_mgr, input, **params):
    items = conn_mgr.run_graph(CLAIMS_WITH_CREDITS, params)
    items = [map_item(item) for item in items]
    items = sorted(items, key=lambda x: x["score"], reverse=True)
    return {"items": items}
