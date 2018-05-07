"""
Filter: Whitelist
=================

Version: 1.0.0

"""

from algorithms.utils import pipeable, filter_debug


@pipeable
@filter_debug
def run(conn_mgr, response, whitelist, **ignore):
    WHITELIST = """
    MATCH (:Entity:Identity { id: {whitelist} })-[:AUTHORED]->(:Entity:Claim)-[:TARGET]->(claim:Entity:Claim)
    RETURN collect(claim.id) as whitelist_ids
    """
    # ^^ Whitelist: all claims that are references by (other) claims by given author

    result = conn_mgr.run_graph(WHITELIST, {
        "whitelist": whitelist
    })

    result = list(result)

    whitelist_ids = result[0]["whitelist_ids"]

    response.update(items=[i for i in response['items'] if i["id"] in whitelist_ids])

    return response
