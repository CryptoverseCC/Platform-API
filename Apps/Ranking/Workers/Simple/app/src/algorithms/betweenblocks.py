"""
Between blocks
==============

Version: 1.0.0

"""

from algorithms.utils import param


LINKS_BETWEEN_BLOCKS = """
MATCH
    (claim:Entity:Claim)-[:CONNECTED_WITH]->(transfer:Transfer { asset: {asset} })-[:RECEIVER]->(:Entity:Identity { id: {context} }),
    (transfer)-[:IN]->(package:Entity:Package),
    (claim)-[:TYPE]->(:Entity:Type { id: 'link' }),
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package:Entity:Package)
WHERE
    {minBlockNumber} <= package.sequence <= {maxBlockNumber}
RETURN
    target.id as target,
    claim.title as title,
    claim.summary as summary,
    claim.id as id,
    transfer.amount as score,
    transfer.amount as total,
    package.timestamp as created_at
UNION
MATCH
    (bid:Entity:Claim)-[:TARGET]->(claim),
    (bid)-[:CONNECTED_WITH]->(transfer:Transfer { asset: {asset} })-[:RECEIVER]->(:Entity:Identity { id: {context} }),
    (transfer)-[:IN]->(package:Entity:Package),
    (claim)-[:TYPE]->(:Entity:Type { id: 'link' }),
    (claim)-[:TARGET]->(target),
    (bid)-[:IN]->(package:Entity:Package)
WHERE
    {minBlockNumber} <= package.sequence <= {maxBlockNumber}
RETURN
    target.id as target,
    claim.title as title,
    claim.summary as summary,
    claim.id as id,
    transfer.amount as score,
    transfer.amount as total,
    package.timestamp as created_at
ORDER BY created_at DESC
"""


@param("context", required=True)
@param("asset", required=True)
@param("minBlockNumber", required=True)
@param("maxBlockNumber", required=True)
def run(conn_mgr, input, **params):
    params["minBlockNumber"] = int(params["minBlockNumber"])
    params["maxBlockNumber"] = int(params["maxBlockNumber"])
    results = conn_mgr.run_graph(LINKS_BETWEEN_BLOCKS, params)
    links = [{
        "target": i["target"],
        "title": i["title"],
        "summary": i["summary"],
        "id": i["id"],
        "score": int(i["score"]),
        "total": int(i["total"]),
        "created_at": i["created_at"],
    } for i in results]

    return {"items": links}
