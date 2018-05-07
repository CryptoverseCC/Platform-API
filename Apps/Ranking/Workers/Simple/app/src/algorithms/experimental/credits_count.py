"""
Credits count
=============

Version: 0.1.0

"""

from algorithms.utils import param

CLAIMS_WITH_CREDITS = "MATCH (c:Claim)-[credit:GIVES_CREDITS]->(e:Entity)"

FILTER_IN_TYPES = "WHERE credit.type IN {type}"

FILTER_EQUALS_TYPE = "WHERE credit.type = {type}"

RETURN = """
RETURN
    credit.type AS credit_type,
    e.id AS credit_value,
    count(*) as group_count
ORDER BY group_count DESC
"""

@param("type", required=False)
def run(conn_mgr, input, **params):
    query = build_query(params)
    query_result = conn_mgr.run_graph(query, params)
    mapped_items = map_items(query_result)
    return {"items": mapped_items}

def build_query(params):
    query = CLAIMS_WITH_CREDITS
    if "type" in params:
        if type(params["type"]) == list:
            query += FILTER_IN_TYPES
        else:
            query += FILTER_EQUALS_TYPE
    query += RETURN
    return query

def map_items(query_result):
    return [map_item(item) for item in query_result]

def map_item(item):
    return {
        "type": item["credit_type"],
        "value": item["credit_value"],
        "group_count": item["group_count"]
    }
