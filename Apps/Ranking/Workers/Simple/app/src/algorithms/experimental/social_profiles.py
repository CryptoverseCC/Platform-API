"""
Social profiles
===============

"""

from algorithms.utils import param

SOCIAL_PROFILES = """
MATCH
    (claim:Claim)-[:LABELS { value: {type} }]->(target),
    (claim)-[:IN]->(package),
    (claim)<-[:AUTHORED]-(author)
OPTIONAL MATCH (claim)-[:CONTEXT]->(context)
WHERE io.userfeeds.erc721.isValidClaim(claim)
RETURN
    claim.id AS id,
    target.id AS target,
    package.sequence AS sequence,
    package.family AS family,
    package.timestamp AS created_at,
    author.id AS author,
    context.id AS context
"""


@param("type", required=True)
def run(conn_mgr, input, **params):
    result = conn_mgr.run_graph(SOCIAL_PROFILES, params)
    return {
        "items": filter_sort_items(result)
    }


def filter_sort_items(result):
    result_map = {}
    for item in result:
        context_or_author = item.get("context") or item["author"]
        if context_or_author not in result_map or result_map[context_or_author]["created_at"] < item["created_at"]:
            result_map[context_or_author] = {
                "id": item["id"],
                "target": item["target"],
                "sequence": item["sequence"],
                "family": item["family"],
                "created_at": item["created_at"],
                "author": item["author"],
                "context": item.get("context")
            }
    return sorted(result_map.values(), key=lambda x: x["created_at"], reverse=True)
