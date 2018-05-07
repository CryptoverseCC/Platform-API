"""
Latest links for linkexchange.io website
========================================

Version: 1.0.0

"""

LATEST_LINKS = """
MATCH (c:Claim)-[:TARGET]->(target:Entity),
  (c)-[:TYPE]->(:Entity { id: 'link' }),
  (c)<-[:AUTHORED]-(author:Identity),
  (c)-[:IN]->(package:PackageEthereum)
OPTIONAL MATCH
  (c)-[:GIVES_CREDITS { type: 'interface' }]->(credits:Entity)
RETURN
  c.id AS id,
  target.id AS target,
  c.title AS title,
  c.summary AS summary,
  author.id AS author,
  package.timestamp AS created_at,
  package.sequence AS sequence,
  credits.id AS publisher
ORDER BY created_at DESC
LIMIT 99
"""


def run(conn_mgr, input, **params):
    results = conn_mgr.run_graph(LATEST_LINKS, params)

    links = [{
        "id": i["id"],
        "target": i["target"],
        "title": i["title"],
        "summary": i["summary"],
        "author": i["author"],
        "created_at": i["created_at"],
        "sequence": i["sequence"],
        "publisher": i["publisher"]
    } for i in results]

    return {"items": links}
