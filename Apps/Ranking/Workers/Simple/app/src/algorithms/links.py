"""
Links
=====

Version: 1.0.0

"""

from algorithms.utils import param


LINKS_IN_CONTEXT = """
MATCH
    (type:Type { id: 'link' }),
    (identity:Identity { id: {context} }),
    (claim:Claim)-[:TYPE]->(type),
    (claim)-[:CONNECTED_WITH]->(transfer:Transfer { asset: {asset} })-[:RECEIVER]->(identity),
    (claim)-[:TARGET]->(target),
    (claim)<-[:AUTHORED]-(author),
    (claim)-[:IN]->(package:Entity:Package)
RETURN
    target.id as target,
    claim.title as title,
    claim.summary as summary,
    claim.id as id,
    transfer.amount as value,
    author.id as author,
    package.timestamp as created_at
UNION
MATCH
    (type:Type { id: 'link' }),
    (identity:Identity { id: {context} }),
    (claim:Claim)-[:TYPE]->(type),
    (bid:Claim)-[:TARGET]->(claim),
    (bid)-[:CONNECTED_WITH]->(transfer:Transfer { asset: {asset} })-[:RECEIVER]->(identity),
    (claim)-[:TARGET]->(target),
    (bid)<-[:AUTHORED]-(author),
    (bid)-[:IN]->(package:Entity:Package)
RETURN
    target.id as target,
    claim.title as title,
    claim.summary as summary,
    claim.id as id,
    transfer.amount as value,
    author.id as author,
    package.timestamp as created_at
"""

LINKS_FOR_ASSET = """
MATCH
    (type:Type {id: 'link'}),
    (claim:Claim)-[:TYPE]->(type),
    (claim)-[:CONNECTED_WITH]->(transfer:Transfer { asset: {asset} }),
    (claim)-[:TARGET]->(target),
    (claim)<-[:AUTHORED]-(author),
    (claim)-[:IN]->(package:Entity:Package)
RETURN
    target.id as target,
    claim.title as title,
    claim.summary as summary,
    claim.id as id,
    transfer.amount as value,
    author.id as author,
    package.timestamp as created_at
UNION
MATCH
    (type:Type {id: 'link'}),
    (claim:Claim)-[:TYPE]->(type),
    (claim)-[:TARGET]->(target),
    (bid:Claim)-[:TARGET]->(claim),
    (bid)-[:CONNECTED_WITH]->(transfer:Transfer { asset: {asset} }),
    (bid)<-[:AUTHORED]-(author),
    (bid)-[:IN]->(package:Entity:Package)
RETURN
    target.id as target,
    claim.title as title,
    claim.summary as summary,
    claim.id as id,
    transfer.amount as value,
    author.id as author,
    package.timestamp as created_at
ORDER BY created_at DESC
"""


@param("context", required=False)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    if 'context' in params:
        results = conn_mgr.run_graph(LINKS_IN_CONTEXT, params)
    else:
        results = conn_mgr.run_graph(LINKS_FOR_ASSET, params)

    links = [{
        "id": i["id"],
        "target": i["target"],
        "title": i["title"],
        "summary": i["summary"],
        "author": i["author"],
        "score": int(i["value"]),
        "total": int(i["value"]),
        "created_at": i["created_at"],
    } for i in results]

    return {"items": links}
