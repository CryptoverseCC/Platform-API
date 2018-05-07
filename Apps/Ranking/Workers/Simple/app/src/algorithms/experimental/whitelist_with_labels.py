"""
Whitelist with labels
=====================

Version: 0.1.0

"""

from algorithms.utils import pipeable, filter_debug


@pipeable
@filter_debug
def run(conn_mgr, response, whitelist, labels, **ignore):
    WHITELIST = """
    MATCH (:Entity:Identity { id: {whitelist} })-[:AUTHORED]->(:Entity:Claim)-[labels:LABELS]->(claim:Entity:Claim)
    WHERE labels.value in {labels}
    RETURN collect(claim.id) as whitelist_ids
    """
    # ^^ Whitelist: all claims that are labeled with given labels by (other) claims by given author

    result = conn_mgr.run_graph(WHITELIST, {
        "whitelist": whitelist,
        "labels": labels
    })

    result = list(result)

    whitelist_ids = result[0]["whitelist_ids"]

    response.update(items=[i for i in response['items'] if i["id"] in whitelist_ids])

    return response
