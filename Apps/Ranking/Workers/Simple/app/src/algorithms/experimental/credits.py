"""
Credits
=======

Version: 0.1.0

"""

CLAIMS_WITH_CREDITS = """
MATCH 
    (c:Claim)-[:IN]->(p:Package),
    (c)-[:TARGET]->(target:Entity),
    (c)-[credit:GIVES_CREDITS]->(e:Entity),
    (c)<-[:AUTHORED]-(author:Identity)
RETURN
    c.id as id,
    target.id as target,
    author.id as author,
    p.family AS family,
    p.sequence AS sequence,
    p.timestamp AS created_at,
    credit.type AS credit_type,
    e.id AS credit_value
"""

def map_item(item):
    return {
        "id": item["id"],
        "target": item["target"],
        "author": item["author"],
        "family": item["family"],
        "sequence": item["sequence"],
        "created_at": item["created_at"],
        "credit_type": item["credit_type"],
        "credit_value": item["credit_value"],
    }

def map_items(query_result):
    return [map_item(item) for item in query_result]

def sorted_by_created_at(query_result):
    return sorted(query_result, key=lambda x: x["created_at"], reverse=True)

def run(conn_mgr, input, **params):
    query_result = conn_mgr.run_graph(CLAIMS_WITH_CREDITS, params)
    mapped_items = map_items(query_result)
    return {"items": sorted_by_created_at(mapped_items)}
